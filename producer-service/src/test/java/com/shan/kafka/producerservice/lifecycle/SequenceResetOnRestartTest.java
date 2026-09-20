package com.shan.kafka.producerservice.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T029 (US2): data-model.md Sequencing rules, spec.md Assumptions — stopping and starting again
 * begins a new run: the first message of the new run has sequenceNumber = 1, sequence numbers
 * within a run increment by exactly 1 with no gaps, and messageId values remain unique across the
 * restart.
 */
class SequenceResetOnRestartTest extends ProducerServiceIntegrationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private Consumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void restartingTheProducerResetsTheSequenceAndKeepsMessageIdsUnique() throws Exception {
        consumer = createTopicConsumer("sequence-reset");

        startProducer();
        List<JsonNode> firstRun = collectEnvelopes(Duration.ofSeconds(2));
        stopProducer();
        assertThat(firstRun).isNotEmpty();

        startProducer();
        List<JsonNode> secondRun = collectEnvelopes(Duration.ofSeconds(2));
        stopProducer();
        assertThat(secondRun).isNotEmpty();

        // First message of the new run is sequenceNumber 1, contiguous thereafter.
        List<Long> secondRunSequences = secondRun.stream()
                .map(node -> node.get("sequenceNumber").asLong())
                .sorted()
                .toList();
        assertThat(secondRunSequences.get(0)).isEqualTo(1);
        for (int i = 1; i < secondRunSequences.size(); i++) {
            assertThat(secondRunSequences.get(i)).isEqualTo(secondRunSequences.get(i - 1) + 1);
        }

        // messageId values remain unique across the restart.
        Set<String> firstRunIds = idsOf(firstRun);
        Set<String> secondRunIds = idsOf(secondRun);
        assertThat(firstRunIds).doesNotContainAnyElementsOf(secondRunIds);
    }

    private List<JsonNode> collectEnvelopes(Duration window) throws Exception {
        List<JsonNode> envelopes = new ArrayList<>();
        for (ConsumerRecord<String, String> record : KafkaTestUtils.getRecords(consumer, window)) {
            envelopes.add(objectMapper.readTree(record.value()));
        }
        return envelopes;
    }

    private static Set<String> idsOf(List<JsonNode> envelopes) {
        Set<String> ids = new HashSet<>();
        for (JsonNode envelope : envelopes) {
            ids.add(envelope.get("messageId").asText());
        }
        return ids;
    }
}
