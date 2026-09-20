package com.shan.kafka.producerservice.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * Bootstrap servers come from application.yaml (spring.kafka.bootstrap-servers), never hardcoded
 * (FR-011, FR-018). The topic name is not bound here: KafkaTemplate.send(topic, value) takes the
 * topic per call, so callers (e.g. the production loop) read it themselves via
 * {@code @Value("${app.kafka.topic}")} rather than this class hardcoding or centralizing it.
 *
 * <p>Both beans are ordinary, method-injected Spring beans (no extra infrastructure) so tests can
 * substitute a fault-injecting {@link KafkaTemplate} double, per research.md R2/T046.
 */
@Configuration
public class ProducerKafkaConfig {

    @Bean
    public ProducerFactory<String, KafkaMessageEnvelope> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                // JacksonJsonSerializer (Jackson 3), not the classic JsonSerializer (Jackson 2,
                // com.fasterxml.jackson.*) — deprecated since Spring Boot 4.0 (research.md R4).
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,
                // consumer-service deserializes into its own, differently-named copy of this
                // type (constitution Principle IV), so a producer-side type header naming
                // producer-service's class would be meaningless to it.
                JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, KafkaMessageEnvelope> kafkaTemplate(
            ProducerFactory<String, KafkaMessageEnvelope> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
