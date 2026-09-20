package com.shan.kafka.consumerservice.kafka;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

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

    @Bean
    public ConsumerFactory<String, KafkaMessageEnvelope> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId) {
        // useHeadersIfPresent=false: always deserialize into this service's own
        // KafkaMessageEnvelope, ignoring any producer-side type header (constitution
        // Principle IV — the two services' copies of this type are not the same class).
        JsonDeserializer<KafkaMessageEnvelope> valueDeserializer =
                new JsonDeserializer<>(KafkaMessageEnvelope.class, false);
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
            ConsumerFactory<String, KafkaMessageEnvelope> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaMessageEnvelope> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
