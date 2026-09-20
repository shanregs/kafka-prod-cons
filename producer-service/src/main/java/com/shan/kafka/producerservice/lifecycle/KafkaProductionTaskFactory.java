package com.shan.kafka.producerservice.lifecycle;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.shan.kafka.producerservice.kafka.KafkaMessageEnvelope;
import com.shan.kafka.producerservice.kafka.KafkaMessageEnvelope.MessagePayload;

/**
 * The real, Kafka-backed {@link ProductionTaskFactory}: a fixed-delay repeating send, period =
 * {@code 1000ms / rate} (no code-level maximum on the rate — FR-009, FR-028, research.md R3). One
 * single-thread scheduler is created per {@link #create(double)} call (i.e., per run) and shut
 * down when the resulting task is cancelled.
 *
 * <p>Each tick is a synchronous send-and-confirm (T031): peek the next sequence number, build and
 * send the envelope with it, block (bounded) for broker confirmation, and only then commit the
 * counter. A failed or timed-out send leaves the counter untouched, so the next tick reuses the
 * same sequence number — accepting the small, documented risk of an occasional duplicate number if
 * a "timed-out" send had, in fact, succeeded (spec.md Assumptions, FR-026: no retries/DLQ/exactly-
 * once to close that gap).
 */
@Component
class KafkaProductionTaskFactory implements ProductionTaskFactory {

    private final KafkaTemplate<String, KafkaMessageEnvelope> kafkaTemplate;
    private final String topic;
    private final String producerId;

    KafkaProductionTaskFactory(
            KafkaTemplate<String, KafkaMessageEnvelope> kafkaTemplate,
            @Value("${app.kafka.topic}") String topic,
            @Value("${spring.application.name}") String producerId) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.producerId = producerId;
    }

    @Override
    public ProductionTask create(double rate) {
        long periodMs = Math.max(1, Math.round(1000.0 / rate));
        // A ProductionTask needs to reference itself (to record outcomes) from inside the very
        // Supplier<AutoCloseable> passed to its own constructor. create(...) is a regular method
        // (not a constructor-chaining call), so this local-holder idiom is legal: the lambda
        // captures the array, not the not-yet-assigned task, and only reads holder[0] later, when
        // the scheduled action actually runs (by which time it's set).
        ProductionTask[] holder = new ProductionTask[1];
        ProductionTask task = new ProductionTask(() -> startSchedule(periodMs, holder));
        holder[0] = task;
        return task;
    }

    private static final long GRACEFUL_SHUTDOWN_SECONDS = 10; // FR-025, SC-011

    private AutoCloseable startSchedule(long periodMs, ProductionTask[] taskHolder) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(
                () -> produceOneMessage(taskHolder[0]), 0, periodMs, TimeUnit.MILLISECONDS);
        return () -> shutdownGracefully(executor);
    }

    /**
     * FR-025/SC-011: stop issuing new messages and let any in-flight tick finish, within 10
     * seconds, before forcibly interrupting it.
     */
    private static void shutdownGracefully(ScheduledExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(GRACEFUL_SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private static final long SEND_CONFIRM_TIMEOUT_SECONDS = 5;

    private void produceOneMessage(ProductionTask task) {
        long sequenceNumber = task.peekNextSequenceNumber();
        KafkaMessageEnvelope envelope = new KafkaMessageEnvelope(
                UUID.randomUUID().toString(),
                Instant.now().toString(),
                producerId,
                sequenceNumber,
                new MessagePayload("producer-service message " + sequenceNumber));

        try {
            kafkaTemplate.send(topic, envelope).get(SEND_CONFIRM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            task.commitSuccessfulSend(sequenceNumber);
        } catch (ExecutionException e) {
            task.recordSendFailure(e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
        } catch (TimeoutException e) {
            // Ambiguous outcome under at-least-once semantics (research.md, T031 note): treated
            // as failed, so the next tick reuses this sequence number.
            task.recordSendFailure("send confirmation timed out after "
                    + SEND_CONFIRM_TIMEOUT_SECONDS + "s");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            task.recordSendFailure("send interrupted");
        }
    }
}
