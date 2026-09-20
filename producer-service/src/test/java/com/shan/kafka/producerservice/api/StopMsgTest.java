package com.shan.kafka.producerservice.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.shan.kafka.producerservice.support.ProducerServiceIntegrationTest;

/**
 * T015 (US1): FR-004, SC-002 — POST /stopmsg from RUNNING transitions to STOPPED and no further
 * messages are produced after stopping.
 */
class StopMsgTest extends ProducerServiceIntegrationTest {

    private Consumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void stoppingARunningProducerTransitionsToStoppedAndHaltsProduction() {
        consumer = createTopicConsumer("stop-msg");

        startProducer();
        // Let a few messages accumulate so the "before" count is unambiguously non-zero.
        assertThat(countRecords(consumer, Duration.ofSeconds(3))).isGreaterThan(0);

        ResponseEntity<Map> response = stopProducerResponse();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsEntry("state", "STOPPED");

        // Drain anything already in flight at the moment of stopping, then confirm nothing new
        // arrives afterward.
        countRecords(consumer, Duration.ofSeconds(2));
        int afterStop = countRecords(consumer, Duration.ofSeconds(3));
        assertThat(afterStop).isZero();
    }
}
