package com.shan.kafka.producerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T028 (US2): SC-005, contracts/kafka-message-contract.md — every produced message contains
 * messageId, producedAt (ISO-8601 UTC), producerId, sequenceNumber, and a non-empty
 * payload.content.
 */
class MessageContractTest extends ProducerServiceIntegrationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private Consumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void everyProducedMessageMatchesTheEnvelopeContract() throws Exception {
        consumer = createTopicConsumer("message-contract");

        startProducer();
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
        stopProducer();

        List<ConsumerRecord<String, String>> recordList = new java.util.ArrayList<>();
        records.forEach(recordList::add);
        assertThat(recordList).isNotEmpty();

        for (ConsumerRecord<String, String> record : recordList) {
            var envelope = objectMapper.readTree(record.value());

            assertThat(envelope.get("messageId").asText()).isNotBlank();
            assertThat(envelope.get("producerId").asText()).isNotBlank();
            assertThat(envelope.get("sequenceNumber").asLong()).isGreaterThanOrEqualTo(1);
            assertThat(envelope.get("payload").get("content").asText()).isNotBlank();

            String producedAt = envelope.get("producedAt").asText();
            // Must parse as an ISO-8601 UTC instant (e.g. ends with "Z").
            assertThat(producedAt).endsWith("Z");
            assertThat(Instant.parse(producedAt)).isNotNull();
        }
    }
}
