# Quickstart: Kafka Rate-Controlled Producer/Consumer Microservices

Validates the feature end-to-end once `producer-service` and `consumer-service` exist per
`plan.md`'s Project Structure. This guide runs the golden path and the key edge cases from
`spec.md`'s acceptance scenarios — it does not contain implementation code.

## Prerequisites

- Java 25 installed (or rely on each service's own toolchain-managed JDK, if configured)
- A Kafka broker reachable at the bootstrap servers each service is configured with (a local
  single-broker Kafka instance is sufficient; how you run that broker — e.g., a local install or a
  container — is not prescribed by this feature)
- No local Maven install required — each service uses its own Maven Wrapper

## Setup

1. Create the Kafka topic referenced by both services' configuration (or rely on broker
   auto-creation, if enabled), matching `kafkaTopic` in `data-model.md`.
2. Start `consumer-service` first (per User Story 3, it starts cleanly with no messages yet):
   ```
   cd consumer-service
   ./mvnw.cmd spring-boot:run
   ```
3. In a second terminal, start `producer-service`:
   ```
   cd producer-service
   ./mvnw.cmd spring-boot:run
   ```

## Golden path validation

1. **Confirm initial state (STOPPED)**:
   ```
   curl http://localhost:<producer-port>/status
   ```
   Expected: `"state": "STOPPED"`, `"messagesProduced": 0` (SC-009).

2. **Start production** (User Story 1, Acceptance Scenario 1):
   ```
   curl -X POST http://localhost:<producer-port>/startmsg
   ```
   Expected: `200 OK`, `"state": "RUNNING"` (SC-001).

3. **Observe rate-controlled production** (User Story 2):
   Wait ~60 seconds, then:
   ```
   curl http://localhost:<producer-port>/status
   ```
   Expected: `messagesProduced` ≈ `configuredRate × elapsed seconds`, within 10% (SC-004, SC-010).

4. **Confirm consumption** (User Story 3):
   ```
   curl http://localhost:<consumer-port>/status
   ```
   Expected: `messagesConsumed` tracking `producer-service`'s `messagesProduced` (allowing for
   in-flight lag), `messagesRejected: 0` for a healthy run.

5. **Inspect a message contract** (User Story 2, Acceptance Scenario 2): consume one record
   directly from the topic (e.g., via any Kafka console consumer pointed at the configured topic
   and bootstrap servers) and confirm it matches `contracts/kafka-message-contract.md`'s schema.

6. **Stop production** (User Story 1, Acceptance Scenario 2):
   ```
   curl -X POST http://localhost:<producer-port>/stopmsg
   ```
   Expected: `200 OK`, `"state": "STOPPED"` (SC-002); `messagesProduced` stops increasing.

## Edge case validation

7. **Idempotent repeat start** (SC-003): call `/startmsg` again while already RUNNING — expect
   `200 OK`, `"state": "RUNNING"`, and no increase in the rate of message production (still exactly
   one production loop).

8. **Idempotent repeat stop** (SC-003): call `/stopmsg` again while already STOPPED — expect
   `200 OK`, `"state": "STOPPED"`, no error.

9. **Concurrent start** (FR-008): fire several `/startmsg` requests at once (e.g., with a simple
   parallel `curl` loop) — expect all `200 OK` with `"state": "RUNNING"`, and `messagesProduced`
   growth consistent with a single production loop, not a multiple.

10. **Invalid/malformed message handling** (SC-008): publish one manually-crafted invalid message
    to the topic (e.g., missing `payload.content`) and confirm via `consumer-service` logs and
    `GET /status` that `messagesRejected` increments and subsequent valid messages still show up in
    `messagesConsumed`.

11. **Broker unavailability** (SC-007): stop the Kafka broker while `producer-service` is RUNNING,
    then check `GET /status` — expect `state` to remain well-defined (not corrupted/unknown) and
    `lastError` to be populated; restart the broker and confirm recovery.

12. **Independent deployability** (SC-006): stop `producer-service` entirely and confirm
    `consumer-service` keeps running without error (and vice versa).

## Automated test-first validation (constitution Principle III)

Each behavior above should first exist as a failing embedded-broker test (research.md R1) in the
relevant service's `src/test/java/...` tree before the corresponding implementation is written —
see `plan.md`'s Project Structure for where those tests live.
