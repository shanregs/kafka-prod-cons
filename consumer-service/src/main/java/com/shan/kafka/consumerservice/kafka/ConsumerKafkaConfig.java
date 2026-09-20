package com.shan.kafka.consumerservice.kafka;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Bootstrap servers, consumer group, and topic all come from application.yaml (FR-011, FR-018),
 * never hardcoded. The value deserializer is wrapped in {@link ErrorHandlingDeserializer}, which
 * catches a malformed record's deserialization failure instead of letting it kill the poll loop —
 * this is the seam T039's container-level error handler builds on to classify such a record as
 * INVALID_DESERIALIZATION rather than crashing consumption of subsequent records (FR-014,
 * kafka-message-contract.md). Ack mode is MANUAL: the listener/error handler (T039+) decides when
 * to acknowledge, including for invalid messages (data-model.md Acknowledgement rule).
 *
 * <p>Both beans are ordinary, method-injected Spring beans (no extra infrastructure) so tests can
 * substitute a fault-injecting double, per research.md R2/T047.
 */
@Configuration
public class ConsumerKafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(ConsumerKafkaConfig.class);

    @Bean
    public ConsumerFactory<String, KafkaMessageEnvelope> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId) {
        // JacksonJsonDeserializer (Jackson 3), not the classic JsonDeserializer (Jackson 2,
        // com.fasterxml.jackson.*) — deprecated since Spring Boot 4.0 (research.md R4).
        // useHeadersIfPresent=false: always deserialize into this service's own
        // KafkaMessageEnvelope, ignoring any producer-side type header (constitution
        // Principle IV — the two services' copies of this type are not the same class).
        JacksonJsonDeserializer<KafkaMessageEnvelope> valueDeserializer =
                new JacksonJsonDeserializer<>(KafkaMessageEnvelope.class, false);
        valueDeserializer.addTrustedPackages(KafkaMessageEnvelope.class.getPackageName());

        Map<String, Object> configProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                // Manual ack (below) means auto-commit must be off.
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(
                configProps,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(valueDeserializer));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaMessageEnvelope> kafkaListenerContainerFactory(
            ConsumerFactory<String, KafkaMessageEnvelope> consumerFactory,
            CommonErrorHandler deserializationErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaMessageEnvelope> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(deserializationErrorHandler);
        // FR-025/SC-011: on shutdown, stop consuming and let in-flight processing/acknowledgment
        // finish, within 10 seconds, before forcing a stop.
        factory.getContainerProperties().setShutdownTimeout(10_000L);
        return factory;
    }

    /**
     * T039's container-level error handler: a malformed record surfaces here as an exception at
     * listener-invocation time (the {@code @KafkaListener} method itself is never called for it —
     * {@link ErrorHandlingDeserializer} defers the failure to this point). Zero retries
     * ({@link FixedBackOff#FixedBackOff(long, long)} with 0 attempts) means the recoverer runs
     * immediately and the container commits past the record — no DLQ, no retry queue (FR-026),
     * matching data-model.md's Acknowledgement rule (every outcome, valid or invalid, is
     * acknowledged).
     */
    @Bean
    public CommonErrorHandler deserializationErrorHandler(ConsumerRuntimeState runtimeState) {
        return new DefaultErrorHandler(
                (record, exception) -> {
                    String detail = rootCauseMessage(exception);
                    log.warn("Discarding unprocessable record at {}-{}@{}: {}",
                            record.topic(), record.partition(), record.offset(), detail);
                    runtimeState.record(ProcessingOutcome.invalidDeserialization(detail));
                },
                new FixedBackOff(0L, 0L)) {

            // T051: broker/connectivity failures with no single failing record (unlike a
            // malformed record, which handleRemaining/the recoverer above already covers)
            // surface here; the consumer client itself retries reconnecting automatically
            // (does not crash) — this just also surfaces the condition via lastError. Does NOT
            // delegate to super: DefaultErrorHandler's default handleOtherException only knows
            // how to handle a small, specific set of exception types and throws
            // IllegalStateException for anything else, which would defeat "does not crash".
            @Override
            public void handleOtherException(Exception thrownException, org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                    org.springframework.kafka.listener.MessageListenerContainer container, boolean batchListener) {
                String detail = rootCauseMessage(thrownException);
                log.warn("Kafka consumer error (connectivity or similar), will keep retrying: {}", detail);
                runtimeState.recordError(detail);
            }
        };
    }

    private static String rootCauseMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() != null ? current.getMessage() : current.toString();
    }
}
