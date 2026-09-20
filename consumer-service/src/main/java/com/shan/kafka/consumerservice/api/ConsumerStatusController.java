package com.shan.kafka.consumerservice.api;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.shan.kafka.consumerservice.kafka.ConsumerRuntimeState;

/**
 * {@code GET /status} per contracts/consumer-status-api.md (FR-021, FR-022, FR-024, SC-009).
 * Served entirely from {@link ConsumerRuntimeState}'s in-memory state — never a live Kafka check.
 * No authentication, same trusted/local-use posture as producer-service's control endpoints
 * (FR-027).
 */
@RestController
public class ConsumerStatusController {

    private final ConsumerRuntimeState runtimeState;

    public ConsumerStatusController(ConsumerRuntimeState runtimeState) {
        this.runtimeState = runtimeState;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("messagesConsumed", runtimeState.getMessagesConsumed());
        body.put("messagesRejected", runtimeState.getMessagesRejected());
        body.put("lastError", runtimeState.getLastError());
        return body;
    }
}
