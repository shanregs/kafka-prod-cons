package com.shan.kafka.producerservice.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T017 (US1): FR-007 — POST /stopmsg while already STOPPED is idempotent: it succeeds (no error)
 * and reports STOPPED, rather than raising an error for "nothing to stop." The shared base's
 * {@code @BeforeEach} already leaves the producer STOPPED, so this test's baseline is exactly the
 * scenario under test.
 */
class StopIdempotencyTest extends ProducerServiceIntegrationTest {

    @Test
    void repeatingStopWhileStoppedSucceedsIdempotently() {
        ResponseEntity<Map> response = stopProducerResponse();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsEntry("state", "STOPPED");
    }
}
