package com.shan.kafka.producerservice.support;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Shared scaffolding for User Story 1/2 embedded-Kafka integration tests: a real HTTP client
 * against a randomly-ported producer-service, backed by an in-process embedded broker
 * (research.md R1), with a valid positive rate configured so /startmsg can actually transition to
 * RUNNING. Each test starts from a guaranteed STOPPED baseline (the underlying Spring context is
 * reused across test methods for speed, so the producer's lifecycle state must be reset by hand
 * rather than by recreating the context).
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "app.producer.rate-per-second=5",
                // @EmbeddedKafka only publishes its address as spring.embedded.kafka.brokers;
                // this bridges it into the property application.yaml actually binds to.
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
        })
@AutoConfigureTestRestTemplate
@EmbeddedKafka(partitions = 1, topics = "${app.kafka.topic:kafka-prod-cons-messages}")
public abstract class ProducerServiceIntegrationTest {

    @Autowired
    protected TestRestTemplate restTemplate;

    @Autowired
    protected EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${app.kafka.topic}")
    protected String topic;

    @BeforeEach
    void ensureStopped() {
        stopProducer();
    }

    protected ResponseEntity<Map> startProducerResponse() {
        return restTemplate.postForEntity("/startmsg", null, Map.class);
    }

    protected ResponseEntity<Map> stopProducerResponse() {
        return restTemplate.postForEntity("/stopmsg", null, Map.class);
    }

    protected Map<String, Object> startProducer() {
        return startProducerResponse().getBody();
    }

    protected Map<String, Object> stopProducer() {
        return stopProducerResponse().getBody();
    }

    /**
     * A fresh, uniquely-grouped consumer positioned at the CURRENT end of the configured topic
     * ({@code auto.offset.reset=latest}), so it only sees messages produced from this point
     * forward. This matters because the embedded broker/topic (and Spring context) are reused
     * across every test extending this base — an "earliest" reset would replay every prior test's
     * leftover messages too, inflating any count-based assertion (e.g. SC-004's rate tolerance).
     * Call this BEFORE starting the producer so the "end" position is established first.
     *
     * <p>Note: {@code auto.offset.reset} alone does NOT achieve this —
     * {@code EmbeddedKafkaBroker#consumeFromAnEmbeddedTopic(consumer, topic)} (no
     * {@code seekToEnd} argument) unconditionally seeks to the beginning regardless of that
     * config; the explicit {@code seekToEnd=true} overload below is what actually matters.
     */
    protected Consumer<String, String> createTopicConsumer(String groupIdSuffix) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-" + groupIdSuffix, "true", embeddedKafkaBroker);
        Consumer<String, String> consumer = new KafkaConsumer<>(
                consumerProps, new StringDeserializer(), new StringDeserializer());
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, true, topic);
        return consumer;
    }

    /**
     * Counts records received over the FULL {@code window}, polling repeatedly rather than
     * returning as soon as one batch arrives (unlike a single {@code KafkaTestUtils.getRecords}
     * call) — needed for any assertion that compares the count against elapsed time (e.g.
     * SC-004's rate tolerance), as opposed to merely checking "at least one message arrived".
     */
    protected int countRecords(Consumer<String, String> consumer, Duration window) {
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
