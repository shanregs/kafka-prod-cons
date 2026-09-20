package com.shan.kafka.consumerservice.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * T045 (US4): FR-021, SC-009 — GET /status returns messagesConsumed, messagesRejected, lastError
 * per contracts/consumer-status-api.md. Needs its own web-enabled context (the shared US3 base
 * uses WebEnvironment.NONE, since most consumer tests don't need HTTP).
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@AutoConfigureTestRestTemplate
@EmbeddedKafka(partitions = 1, topics = "${app.kafka.topic:kafka-prod-cons-messages}")
class ConsumerStatusTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void statusReturnsCountsAndLastError() {
        ResponseEntity<Map> response = restTemplate.getForEntity("/status", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map body = response.getBody();
        assertThat(body).containsKey("messagesConsumed");
        assertThat(body).containsKey("messagesRejected");
        assertThat(body).containsKey("lastError");
    }
}
