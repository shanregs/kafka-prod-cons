package com.shan.kafka.consumerservice.support;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.shan.kafka.consumerservice.kafka.ConsumerRuntimeState;

/**
 * Shared scaffolding for User Story 3 embedded-Kafka tests: publishes raw records directly to the
 * topic (bypassing producer-service entirely, per FR-012) and asserts against
 * {@link ConsumerRuntimeState} rather than an HTTP status endpoint (that's US4/T049 — out of scope
 * here). No web server is started ({@code WebEnvironment.NONE}) since these tests don't need HTTP.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
        })
@EmbeddedKafka(partitions = 1, topics = "${app.kafka.topic:kafka-prod-cons-messages}")
public abstract class ConsumerServiceIntegrationTest {

    @Autowired
    protected EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    protected ConsumerRuntimeState runtimeState;

    @Value("${app.kafka.topic}")
    protected String topic;

    /** Publishes an arbitrary raw string value — used for both valid JSON and deliberately
     * malformed payloads. */
    protected void publishRaw(String value) throws ExecutionException, InterruptedException {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        try (Producer<String, String> producer = new KafkaProducer<>(
                producerProps, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, value)).get();
        }
    }

    protected void publishValidEnvelope(long sequenceNumber, String content) throws Exception {
        String json = """
                {"messageId":"%s","producedAt":"%s","producerId":"test-producer","sequenceNumber":%d,"payload":{"content":"%s"}}
                """.formatted(
                java.util.UUID.randomUUID(),
                java.time.Instant.now(),
                sequenceNumber,
                content);
        publishRaw(json.strip());
    }

    /** Polls the runtime-state counters until they satisfy {@code condition} or time out. */
    protected void awaitCondition(java.util.function.BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
    }
}
