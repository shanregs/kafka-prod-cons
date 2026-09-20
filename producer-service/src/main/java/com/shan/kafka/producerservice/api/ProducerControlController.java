package com.shan.kafka.producerservice.api;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.shan.kafka.producerservice.lifecycle.ProducerLifecycle;

/**
 * {@code POST /startmsg} / {@code POST /stopmsg} per contracts/producer-api.md: 200 for
 * success/idempotent on both endpoints, 409 for invalid configuration on start. No
 * authentication/authorization (FR-027).
 */
@RestController
public class ProducerControlController {

    private final ProducerLifecycle lifecycle;

    public ProducerControlController(ProducerLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @PostMapping("/startmsg")
    public ResponseEntity<Map<String, Object>> startmsg() {
        ProducerLifecycle.StartResult result = lifecycle.start();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("state", result.state().name());

        if (result.isInvalid()) {
            body.put("error", result.error());
            return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
        }

        body.put("configuredRate", result.configuredRate());
        return ResponseEntity.ok(body);
    }

    @PostMapping("/stopmsg")
    public ResponseEntity<Map<String, Object>> stopmsg() {
        lifecycle.stop();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("state", ProducerLifecycle.State.STOPPED.name());
        return ResponseEntity.ok(body);
    }

    /**
     * T048/FR-019/FR-020/FR-023/SC-009: served entirely from in-memory state, never a live Kafka
     * check — see {@link ProducerLifecycle}'s getters.
     */
    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("state", lifecycle.getState().name());
        body.put("configuredRate", lifecycle.getConfiguredRate());
        body.put("messagesProduced", lifecycle.getMessagesProduced());
        body.put("lastSequenceNumber", lifecycle.getLastSequenceNumber());
        body.put("lastError", lifecycle.getLastError());
        return body;
    }
}
