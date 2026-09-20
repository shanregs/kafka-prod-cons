package com.shan.kafka.producerservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * T027 (US2): FR-028, SC-010 — with the configured rate at 10 msg/s (the acceptance floor), the
 * rate holds within the SC-004 10% tolerance for the FULL 5-consecutive-minute window without
 * resource exhaustion or lifecycle state corruption. This duration is intentionally not shortened
 * (per the explicit instruction preserved in tasks.md T027) — it is tagged {@code slow} and
 * excluded from the default {@code mvnw.cmd test} run; run it explicitly via:
 * {@code mvnw.cmd test -Pslow-tests -Dtest=SustainedRateTest}.
 */
@Tag("slow")
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "app.producer.rate-per-second=10",
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
        })
@AutoConfigureTestRestTemplate
@EmbeddedKafka(partitions = 1, topics = "${app.kafka.topic:kafka-prod-cons-messages}")
class SustainedRateTest {

    private static final long CONFIGURED_RATE_PER_SECOND = 10;
    private static final Duration WINDOW = Duration.ofMinutes(5);

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${app.kafka.topic}")
    private String topic;

    private Consumer<String, String> consumer;

    @AfterEach
    void cleanup() {
        restTemplate.postForEntity("/stopmsg", null, Map.class);
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void sustainsFloorRateForFiveMinutesWithoutCorruption() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "sustained-rate-test", "true", embeddedKafkaBroker);
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer());
        // seekToEnd=true: auto.offset.reset alone would not skip prior tests' backlog (see
        // ProducerServiceIntegrationTest.createTopicConsumer's note).
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, true, topic);

        Map<String, Object> startResponse = restTemplate.postForEntity("/startmsg", null, Map.class).getBody();
        assertThat(startResponse).containsEntry("state", "RUNNING");

        int observed = countRecordsOverFullWindow(consumer, WINDOW);

        Map<String, Object> stillRunning = restTemplate.postForEntity("/startmsg", null, Map.class).getBody();
        assertThat(stillRunning).containsEntry("state", "RUNNING"); // lifecycle not corrupted

        long expected = CONFIGURED_RATE_PER_SECOND * WINDOW.toSeconds();
        long tolerance = Math.round(expected * 0.10);
        assertThat(observed).isBetween((int) (expected - tolerance), (int) (expected + tolerance));
    }

    /**
     * Polls repeatedly for the FULL window rather than returning as soon as one batch arrives
     * (a single {@code KafkaTestUtils.getRecords} call does the latter, which undercounts here).
     */
    private static int countRecordsOverFullWindow(Consumer<String, String> consumer, Duration window) {
        long deadlineNanos = System.nanoTime() + window.toNanos();
        int total = 0;
        while (System.nanoTime() < deadlineNanos) {
            long remainingMs = (deadlineNanos - System.nanoTime()) / 1_000_000;
            long pollMs = Math.max(0, Math.min(500, remainingMs));
            total += KafkaTestUtils.getRecords(consumer, Duration.ofMillis(pollMs)).count();
        }
        return total;
    }
}
