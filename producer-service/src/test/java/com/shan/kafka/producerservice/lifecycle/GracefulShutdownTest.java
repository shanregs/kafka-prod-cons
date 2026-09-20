package com.shan.kafka.producerservice.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T054 (Polish): FR-025, SC-011 — on a shutdown signal, producer-service stops producing new
 * messages and the process exits within 10 seconds. Calls {@link ProducerLifecycle#shutdown()}
 * directly — the exact {@code @PreDestroy} method Spring invokes on real context shutdown —
 * rather than closing the whole (shared, test-context-cached) Spring context, which would
 * conflict with Spring's own context-cache teardown.
 */
class GracefulShutdownTest extends ProducerServiceIntegrationTest {

    @Autowired
    private ProducerLifecycle producerLifecycle;

    private Consumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void shutdownStopsProductionWithinTenSeconds() {
        consumer = createTopicConsumer("graceful-shutdown");
        startProducer();
        assertThat(countRecords(consumer, Duration.ofSeconds(2))).isGreaterThan(0);

        long start = System.nanoTime();
        producerLifecycle.shutdown();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs).isLessThan(10_000);
        assertThat(producerLifecycle.getState()).isEqualTo(ProducerLifecycle.State.STOPPED);

        // Drain anything already in flight, then confirm nothing new arrives after shutdown.
        countRecords(consumer, Duration.ofSeconds(1));
        assertThat(countRecords(consumer, Duration.ofSeconds(3))).isZero();
    }
}
