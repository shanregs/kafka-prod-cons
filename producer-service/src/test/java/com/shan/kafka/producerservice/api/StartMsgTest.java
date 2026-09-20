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
 * T014 (US1): FR-003, FR-005 — POST /startmsg from STOPPED transitions to RUNNING and begins
 * producing messages to the configured topic.
 */
class StartMsgTest extends ProducerServiceIntegrationTest {

    private Consumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void startingAStoppedProducerTransitionsToRunningAndProducesMessages() {
        consumer = createTopicConsumer("start-msg");

        ResponseEntity<Map> response = startProducerResponse();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsEntry("state", "RUNNING");
        assertThat(countRecords(consumer, Duration.ofSeconds(10))).isGreaterThan(0);
    }
}
