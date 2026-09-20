package com.shan.kafka.producerservice.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T016 (US1): FR-006 — POST /startmsg while already RUNNING is idempotent: it succeeds (no error)
 * and reports the existing RUNNING state, rather than being rejected or starting a second loop.
 */
class StartIdempotencyTest extends ProducerServiceIntegrationTest {

    @Test
    void repeatingStartWhileRunningSucceedsIdempotently() {
        ResponseEntity<Map> first = startProducerResponse();
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody()).containsEntry("state", "RUNNING");

        ResponseEntity<Map> second = startProducerResponse();
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody()).containsEntry("state", "RUNNING");
    }
}
