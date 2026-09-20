package com.shan.kafka.producerservice.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T044 (US4): FR-019, FR-020, SC-009 — GET /status returns state, configuredRate,
 * messagesProduced, lastSequenceNumber, lastError per contracts/producer-api.md.
 */
class ProducerStatusTest extends ProducerServiceIntegrationTest {

    @Test
    void statusReflectsCurrentLifecycleAndCounters() {
        ResponseEntity<Map> stoppedStatus = restTemplate.getForEntity("/status", Map.class);
        assertThat(stoppedStatus.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(stoppedStatus.getBody()).containsEntry("state", "STOPPED");

        startProducer();

        ResponseEntity<Map> runningStatus = restTemplate.getForEntity("/status", Map.class);
        assertThat(runningStatus.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map body = runningStatus.getBody();
        assertThat(body).containsEntry("state", "RUNNING");
        assertThat(body).containsKey("configuredRate");
        assertThat(body).containsKey("messagesProduced");
        assertThat(body).containsKey("lastSequenceNumber");
        assertThat(body).containsKey("lastError");
    }
}
