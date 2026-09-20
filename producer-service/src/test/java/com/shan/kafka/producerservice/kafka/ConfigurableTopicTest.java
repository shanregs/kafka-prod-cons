package com.shan.kafka.producerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * T030 (US2): FR-011 — changing the configured Kafka topic causes messages to be produced to the
 * newly configured topic without a code change. Uses a non-default topic name to prove it isn't
 * hardcoded anywhere in {@code ProducerKafkaConfig}/{@code KafkaProductionTaskFactory}.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "app.producer.rate-per-second=5",
                "app.kafka.topic=a-custom-topic-name",
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
        })
@AutoConfigureTestRestTemplate
@EmbeddedKafka(partitions = 1, topics = "a-custom-topic-name")
class ConfigurableTopicTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<String, String> consumer;

    @AfterEach
    void cleanup() {
        restTemplate.postForEntity("/stopmsg", null, Map.class);
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void messagesAreProducedToTheConfiguredTopic() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "configurable-topic-test", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer());
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "a-custom-topic-name");

        restTemplate.postForEntity("/startmsg", null, Map.class);

        int observed = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10)).count();
        assertThat(observed).isGreaterThan(0);
    }
}
