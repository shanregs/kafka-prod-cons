package com.shan.kafka.producerservice.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * T020 (US1): Edge Cases, data-model.md Validation rules — POST /startmsg with an invalid
 * configured rate (zero, negative, or missing) returns 409 Conflict with {@code state: "STOPPED"}
 * and never transitions to RUNNING, in all three cases. The rate is bound once at service startup
 * (T005/T024), so each case needs its own Spring context rather than being switchable at runtime.
 */
// A literal topic name, not a property placeholder: @EmbeddedKafka on this outer,
// never-directly-instantiated class doesn't have a bootstrapped Spring Environment available to
// resolve "${app.kafka.topic:...}" against (unlike ProducerServiceIntegrationTest, which is
// itself extended by a concrete, directly-run @SpringBootTest class) — it must match
// application.yaml's default (KAFKA_TOPIC unset in tests).
@EmbeddedKafka(partitions = 1, topics = "kafka-prod-cons-messages")
class InvalidRateTest {

    private static final String BOOTSTRAP_SERVERS_BRIDGE =
            "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}";

    private static void assertRejected(TestRestTemplate restTemplate) {
        ResponseEntity<Map> response = restTemplate.postForEntity("/startmsg", null, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody()).containsEntry("state", "STOPPED");
    }

    @Nested
    @SpringBootTest(
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {"app.producer.rate-per-second=0", BOOTSTRAP_SERVERS_BRIDGE})
    @AutoConfigureTestRestTemplate
    class ZeroRate {

        @Autowired
        private TestRestTemplate restTemplate;

        @Test
        void zeroRateIsRejected() {
            assertRejected(restTemplate);
        }
    }

    @Nested
    @SpringBootTest(
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {"app.producer.rate-per-second=-1", BOOTSTRAP_SERVERS_BRIDGE})
    @AutoConfigureTestRestTemplate
    class NegativeRate {

        @Autowired
        private TestRestTemplate restTemplate;

        @Test
        void negativeRateIsRejected() {
            assertRejected(restTemplate);
        }
    }

    @Nested
    @SpringBootTest(
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = BOOTSTRAP_SERVERS_BRIDGE)
    @AutoConfigureTestRestTemplate
    // No app.producer.rate-per-second override: exercises T005's no-default binding, i.e. the
    // property is entirely absent (not merely "0") — the context must still start (T005/T024).
    class MissingRate {

        @Autowired
        private TestRestTemplate restTemplate;

        @Test
        void missingRateIsRejected() {
            assertRejected(restTemplate);
        }
    }
}
