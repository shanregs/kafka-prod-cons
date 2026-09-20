# Tasks: Kafka Rate-Controlled Producer/Consumer Microservices

**Input**: Design documents from `/specs/001-kafka-producer-consumer/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md (all present)

**Tests**: Included and test-first, per constitution Principle III (NON-NEGOTIABLE) — each user
story's tests are written before its implementation tasks and must fail first.

**Organization**: Tasks are grouped by user story (P1–P4 from spec.md) so each story is
independently implementable and testable. `producer-service` and `consumer-service` remain two
separate, independently buildable Maven projects throughout — no task adds REST/RPC between them,
a database, retry/DLQ infrastructure, a schema registry, exactly-once processing, authentication,
or multi-instance consumer scaling, per the constitution and FR-002/FR-026/FR-027/FR-029.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependency on an incomplete task)
- **[Story]**: Which user story this task belongs to (US1–US4)
- File paths are exact and match `plan.md`'s Project Structure

---

## Phase 1: Setup

**Purpose**: Stand up the two independently deployable Maven projects, each with its own Maven
Wrapper, per constitution Principle IV and the Technology Stack Constraints (Java 25, Spring Boot
4.1.1, Maven Wrapper — no local Maven install required).

- [X] T001 [P] Create `producer-service/` as a new Maven project: `pom.xml` (parent
  `spring-boot-starter-parent` 4.1.1, `java.version` 25, empty `<license>`/`<developers>`/`<scm>`
  overrides matching the retired root skeleton's convention) plus its own Maven Wrapper
  (`producer-service/mvnw`, `producer-service/mvnw.cmd`, `producer-service/.mvn/`)
- [X] T002 [P] Create `consumer-service/` as a new Maven project: `pom.xml` (parent
  `spring-boot-starter-parent` 4.1.1, `java.version` 25, same empty overrides) plus its own Maven
  Wrapper (`consumer-service/mvnw`, `consumer-service/mvnw.cmd`, `consumer-service/.mvn/`)
- [X] T003 Add dependencies to `producer-service/pom.xml`: `spring-boot-starter-webmvc`,
  `spring-boot-starter-kafka`, `spring-boot-starter-actuator`, and (test scope)
  `spring-boot-starter-webmvc-test`, `spring-boot-starter-kafka-test` — per plan.md Primary
  Dependencies and research.md R1/R5. Also configure the Surefire plugin to exclude a `slow`
  JUnit 5 tag by default and add a `slow-tests` Maven profile (or equivalent tag-based Failsafe
  binding) that includes it, so the default `mvnw.cmd test` stays fast while the long-running test
  from T027 still runs under an explicit command (T027 depends on this)
- [X] T004 [P] Add dependencies to `consumer-service/pom.xml`: `spring-boot-starter-kafka`,
  `spring-boot-starter-webmvc` (needed for `consumer-service`'s own `GET /status` REST endpoint,
  `contracts/consumer-status-api.md` — corrected during implementation; plan.md originally scoped
  webmvc to `producer-service` only, which missed the consumer's status endpoint), and
  `spring-boot-starter-actuator`, plus (test scope) `spring-boot-starter-webmvc-test`,
  `spring-boot-starter-kafka-test` — per plan.md Primary Dependencies and research.md R1/R5
- [X] T005 `producer-service/src/main/resources/application.yaml`: externalize
  `spring.kafka.bootstrap-servers` and the production Kafka topic as environment-variable-
  overridable settings with local-dev defaults (e.g. `${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}`),
  per research.md R6 and FR-018. Bind the messages-per-second rate **without a valid-number
  default** (e.g. `${PRODUCER_RATE_PER_SECOND:}`, binding to an empty/absent value rather than a
  required property) so that an unset or blank rate reaches the application as a representable
  "no rate configured" runtime value instead of failing Spring Boot startup — this is what lets
  T020/T024 validate a missing rate at `/startmsg` time rather than the service never starting
  (data-model.md `ProductionRateConfiguration`)
- [X] T006 [P] `consumer-service/src/main/resources/application.yaml`: externalize
  `spring.kafka.bootstrap-servers`, the Kafka topic (must match the producer's), and the consumer
  group ID as environment-variable-overridable settings, per research.md R6, FR-018, and
  data-model.md `ConsumerConfiguration`
- [X] T007 Remove the retired root-level single-module skeleton (`pom.xml`, `mvnw`, `mvnw.cmd`,
  `.mvn/`, `src/`) at the repository root, per plan.md's Structure Decision — its content (base
  package `com.shan.kafka.kafkaprodcons`, single combined app) is superseded by
  `producer-service/` and `consumer-service/`

**Checkpoint**: Two independent, empty-but-buildable Spring Boot projects exist, each with its own
Maven Wrapper and externalized configuration.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared-shape code every user story depends on. Each service implements its own copy
of the message envelope — there is no shared module between the two services (constitution
Principle IV).

**CRITICAL**: No user story task may begin until this phase is complete.

- [ ] T008 [P] Create `ProducerServiceApplication` main class in
  `producer-service/src/main/java/com/shan/kafka/producerservice/ProducerServiceApplication.java`
- [ ] T009 [P] Create `ConsumerServiceApplication` main class in
  `consumer-service/src/main/java/com/shan/kafka/consumerservice/ConsumerServiceApplication.java`
- [ ] T010 [P] Implement the `KafkaMessageEnvelope`/`MessagePayload` types in
  `producer-service/src/main/java/com/shan/kafka/producerservice/kafka/KafkaMessageEnvelope.java`,
  matching `contracts/kafka-message-contract.md` exactly: `messageId` (string, required),
  `producedAt` (ISO-8601 timestamp, always UTC, required), `producerId` (string, required),
  `sequenceNumber` (integer, ≥ 1, required), `payload.content` (string, required, non-empty)
- [ ] T011 [P] Implement the mirrored `KafkaMessageEnvelope`/`MessagePayload` deserialization types
  in
  `consumer-service/src/main/java/com/shan/kafka/consumerservice/kafka/KafkaMessageEnvelope.java`,
  matching the same contract fields and constraints as T010
- [ ] T012 Configure `producer-service`'s Kafka producer (JSON value serializer; topic and
  bootstrap servers bound from `application.yaml`, never hardcoded — FR-011, FR-018) as a regular,
  constructor-injected Spring bean, in
  `producer-service/src/main/java/com/shan/kafka/producerservice/kafka/ProducerKafkaConfig.java`
  (depends on: T005, T010) — kept as an ordinary injectable bean (no extra infrastructure) so
  T046's test can substitute a fault-injecting test double for it
- [ ] T013 [P] Configure `consumer-service`'s Kafka consumer (JSON value deserializer; topic,
  bootstrap servers, and consumer group bound from `application.yaml`; manual acknowledgment mode
  per data-model.md's Acknowledgement rule) as a regular, constructor-injected Spring bean, in
  `consumer-service/src/main/java/com/shan/kafka/consumerservice/kafka/ConsumerKafkaConfig.java`
  (depends on: T006, T011) — kept as an ordinary injectable bean so T047's test can substitute a
  fault-injecting test double for it

**Checkpoint**: Foundation ready — user story work can begin.

---

## Phase 3: User Story 1 - Control Message Production Lifecycle (Priority: P1) — MVP

**Goal**: Race-free `POST /startmsg` / `POST /stopmsg` lifecycle control on `producer-service`,
with no duplicate production loops and no possibility of a task activating after STOPPED.

**Independent Test**: Call start/stop (including repeated and concurrent calls) against
`producer-service` alone; observe its reported lifecycle state and whether messages appear on the
configured Kafka topic. No consumer required.

### Tests for User Story 1 (write first; must fail before implementation)

- [ ] T014 [P] [US1] Embedded-Kafka test: `POST /startmsg` from STOPPED returns `200 OK`
  `{"state":"RUNNING", ...}` and messages begin appearing on the configured topic (FR-003, FR-005)
  in `producer-service/src/test/java/com/shan/kafka/producerservice/api/StartMsgTest.java`
- [ ] T015 [P] [US1] Embedded-Kafka test: `POST /stopmsg` from RUNNING returns `200 OK`
  `{"state":"STOPPED"}` and no further messages are produced after stopping (FR-004, SC-002) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/api/StopMsgTest.java`
- [ ] T016 [P] [US1] Test: `POST /startmsg` while already RUNNING is idempotent — `200 OK`,
  `state: "RUNNING"`, no second production loop created (FR-006) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/lifecycle/StartIdempotencyTest.java`
- [ ] T017 [P] [US1] Test: `POST /stopmsg` while already STOPPED is idempotent — `200 OK`,
  `state: "STOPPED"`, no error (FR-007) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/lifecycle/StopIdempotencyTest.java`
- [ ] T018 [US1] Concurrency test: firing several simultaneous `POST /startmsg` calls results in
  exactly one active production loop and a consistently RUNNING reported state (FR-008, SC-003) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/lifecycle/ConcurrentStartTest.java`
- [ ] T019 [US1] Deterministic race test against `ProducerLifecycle`/`ProductionTask` directly
  (not through HTTP timing, which cannot reliably force this interleaving): (a) an explicit-ordering
  sub-test that drives the two orderings by direct method calls with no real concurrency —
  install→cancel→activate (asserting the later activate call is a no-op and no execution is ever
  scheduled) and install→activate→cancel (asserting the scheduled execution is stopped) — proving
  the one-time, mutually-exclusive activate/cancel transition from research.md R2 step 2 in
  isolation; and (b) a concurrent-thread confirmation that reproduces R2's exact race under real
  threads using a test-only synchronization hook/barrier (e.g., a latch the production task's
  activation path signals/waits on) to force start's install to win, then let a concurrent stop's
  clear-and-cancel run to completion, before releasing start's thread to call activate — asserting
  activate is a no-op and zero production occurs while `state` is reported STOPPED (research.md R2,
  FR-008) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/lifecycle/StartStopRaceTest.java`
- [ ] T020 [P] [US1] Test: `POST /startmsg` with an invalid configured rate — covering all three
  cases: zero, negative, **and** missing (the rate property left unset/blank via test
  configuration, per T005's binding, so the application context still starts normally) — returns
  `409 Conflict` with `state: "STOPPED"` and does not transition to RUNNING in each case (Edge
  Cases, data-model.md Validation rules) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/api/InvalidRateTest.java`

### Implementation for User Story 1

- [ ] T021 [US1] Implement `ProducerLifecycle` (`state`, `configuredRate`, `startedAt`,
  `messagesProduced`, `lastSequenceNumber`, `lastError`, initial state STOPPED — FR-005,
  data-model.md `ProducerLifecycleState`) in
  `producer-service/src/main/java/com/shan/kafka/producerservice/lifecycle/ProducerLifecycle.java`
  (depends on: T008)
- [ ] T022 [US1] Implement `ProductionTask` with the install-then-activate/cancel protocol from
  research.md R2: an inert task installed via one compare-and-set of a shared slot, then a single
  one-time, mutually exclusive activate/cancel transition on the task itself, in
  `producer-service/src/main/java/com/shan/kafka/producerservice/lifecycle/ProductionTask.java`
  (depends on: T021)
- [ ] T023 [US1] Implement the fixed-delay rate-controlled scheduling loop inside `ProductionTask`
  (period = `1000ms / configuredRate`, no code-level maximum on the configured rate — FR-009,
  FR-028, research.md R3), sending one `KafkaMessageEnvelope` per execution via the producer from
  T012 (depends on: T010, T012, T022)
- [ ] T024 [US1] Implement invalid-configuration validation in `ProducerLifecycle`: read the rate
  configuration bound per T005 (which is absent/blank rather than startup-failing when unset) and
  treat "absent," "zero," and "negative" identically — a start request in any of these cases MUST
  NOT transition to RUNNING and MUST set `lastError`, so a missing rate reaches this same runtime
  validation path a zero/negative rate does, rather than ever preventing the service from starting
  (data-model.md Validation rules, Edge Cases) (depends on: T021)
- [ ] T025 [US1] Implement `ProducerControlController` exposing `POST /startmsg` and
  `POST /stopmsg` per `contracts/producer-api.md` response shapes (`200` success/idempotent for
  both endpoints, `409` for invalid configuration on start) in
  `producer-service/src/main/java/com/shan/kafka/producerservice/api/ProducerControlController.java`
  (depends on: T021, T024)

**Checkpoint**: User Story 1 is fully functional and independently testable — lifecycle control
works correctly under repeated and concurrent start/stop calls, with a bare production loop already
running.

---

## Phase 4: User Story 2 - Produce Rate-Controlled, Traceable Messages (Priority: P2)

**Goal**: The production loop from US1 hits its configured rate within tolerance, every message
carries a complete, correctly-sequenced envelope, and the topic is configurable.

**Independent Test**: Configure a target rate, start the producer, and over an observation window
measure messages landing on the configured topic and inspect their contents. No consumer required.

### Tests for User Story 2 (write first; must fail before implementation)

- [ ] T026 [P] [US2] Embedded-Kafka rate test: with a configured rate N, over the SC-004 60-second
  window, the aggregate produced count is within 10% of N (FR-016, FR-017, SC-004) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/kafka/RateAccuracyTest.java`
- [ ] T027 [P] [US2] Embedded-Kafka floor test: with the configured rate at 10 msg/s, the rate
  holds within the SC-004 tolerance for the **full** SC-010 5-consecutive-minute window without
  resource exhaustion or lifecycle state corruption (FR-028, SC-010) — the full 5-minute duration
  MUST NOT be shortened or weakened; tag this test `slow` (per T003's Surefire/profile setup) so it
  is excluded from the default `mvnw.cmd test` run and instead executes under the dedicated
  `slow-tests` profile/command in
  `producer-service/src/test/java/com/shan/kafka/producerservice/kafka/SustainedRateTest.java`
  (depends on: T003)
- [ ] T028 [P] [US2] Embedded-Kafka contract test: every produced message contains `messageId`,
  `producedAt` (ISO-8601, UTC), `producerId`, `sequenceNumber`, and a non-empty `payload.content`,
  per `contracts/kafka-message-contract.md` (SC-005) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/kafka/MessageContractTest.java`
- [ ] T029 [P] [US2] Test: stopping and starting again begins a new run — the first message of the
  new run has `sequenceNumber = 1`, sequence numbers within a run increment by exactly 1 with no
  gaps, and `messageId` values remain unique across the restart (data-model.md Sequencing rules,
  spec.md Assumptions) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/lifecycle/SequenceResetOnRestartTest.java`
- [ ] T030 [P] [US2] Test: changing the configured Kafka topic causes messages to be produced to
  the newly configured topic without a code change (FR-011) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/kafka/ConfigurableTopicTest.java`

### Implementation for User Story 2

- [ ] T031 [US2] Implement `ProductionTask`'s per-tick send algorithm, via a **synchronous
  send-and-confirm per scheduled tick** (the simplest option compatible with the FR-028 ≥10 msg/s
  floor — a fixed-delay tick fires only once per period, so waiting for confirmation within that
  period needs no async-callback reconciliation logic):
  1. Determine the next sequence number as `lastSequenceNumber + 1` (or `1` if this is the first
     message of the current run).
  2. Build the complete `KafkaMessageEnvelope` — unique `messageId`, `producedAt` as the current
     UTC timestamp, `producerId` from configuration, and that sequence number — **before** sending;
     the sequence number MUST already be part of the envelope that goes to Kafka, not applied after
     the fact.
  3. Send the envelope and wait (bounded) for the Kafka producer to confirm the send.
  4. Only after a confirmed successful send, update `ProducerLifecycle`'s `messagesProduced` and
     `lastSequenceNumber` to that sequence number.
  5. If the send fails or times out, do NOT advance `messagesProduced` or `lastSequenceNumber` —
     the next tick recomputes and reuses the same next sequence number from step 1 (data-model.md
     Sequencing rules: no number consumed on a failed send, no gaps within a run).

  Note: a producer-side timeout can leave the actual broker outcome ambiguous under at-least-once
  semantics (the send may have actually succeeded on the broker even though the confirmation never
  arrived) — resolving that ambiguity would require exactly-once/idempotent-producer guarantees,
  which are explicitly out of scope (FR-026); on a timeout this task treats the send as failed per
  step 5 and reuses the sequence number, accepting the small resulting risk of an occasional
  duplicate `sequenceNumber`/message pair on the topic rather than adding retries, a DLQ, or
  exactly-once processing to close it, in `ProductionTask`'s send path (depends on: T023)
- [ ] T032 [US2] Reset `ProducerLifecycle`'s sequence/produced-count state at each STOPPED→RUNNING
  transition (not on an idempotent no-op start) per data-model.md Sequencing rules (depends on:
  T021, T031)
- [ ] T033 [US2] Confirm the Kafka topic used by `ProducerKafkaConfig` is read only from
  `application.yaml`/environment, never hardcoded (FR-011) (depends on: T012)

**Checkpoint**: User Stories 1 and 2 both work independently — lifecycle control plus verified
rate-controlled, fully-specified message production.

---

## Phase 5: User Story 3 - Consume and Process Produced Messages (Priority: P3)

**Goal**: `consumer-service` reliably consumes, validates, processes/logs, and acknowledges
messages from the configured topic, handling invalid messages without stalling.

**Independent Test**: Publish messages to the configured topic (with or without `producer-service`
running) and run `consumer-service` on its own; verify via logs/status that each message was
received, validated, processed, and acknowledged.

### Tests for User Story 3 (write first; must fail before implementation)

- [ ] T034 [P] [US3] Embedded-Kafka test: a valid message is consumed, logged, and increments
  `messagesConsumed` (FR-013, FR-015) in
  `consumer-service/src/test/java/com/shan/kafka/consumerservice/kafka/ValidMessageConsumptionTest.java`
- [ ] T035 [P] [US3] Embedded-Kafka test: publish a record whose value is not valid JSON at all (so
  deserialization fails before a `KafkaMessageEnvelope` object can exist, and the `@KafkaListener`
  method body is never invoked for that record) followed by a valid message; assert the malformed
  record is classified `INVALID_DESERIALIZATION`, logged, and acknowledged/committed by the
  container-level error handling from T039, and that the next, valid message is still received and
  processed normally (FR-014, SC-008, kafka-message-contract.md Delivery semantics) in
  `consumer-service/src/test/java/com/shan/kafka/consumerservice/kafka/MalformedMessageTest.java`
- [ ] T036 [P] [US3] Embedded-Kafka test: a message that deserializes but fails validation (a
  missing envelope field, or a missing/empty `payload.content`) is logged/handled as
  `INVALID_VALIDATION`, acknowledged/committed, and does not stop consumption of the next valid
  message (FR-014, SC-008) in
  `consumer-service/src/test/java/com/shan/kafka/consumerservice/kafka/InvalidPayloadMessageTest.java`
- [ ] T037 [P] [US3] Test: `consumer-service` starts cleanly with no messages on the topic and no
  `producer-service` running or reachable (FR-012) in
  `consumer-service/src/test/java/com/shan/kafka/consumerservice/ConsumerStartupTest.java`
- [ ] T038 [P] [US3] Test: on restart, already-acknowledged messages (valid or invalid) are not
  redelivered; only messages left unacknowledged before the restart may be redelivered and
  reprocessed (kafka-message-contract.md Delivery semantics) in
  `consumer-service/src/test/java/com/shan/kafka/consumerservice/kafka/RedeliveryOnRestartTest.java`

### Implementation for User Story 3

- [ ] T039 [US3] Implement a `@KafkaListener` consuming from the configured topic and deserializing
  into `KafkaMessageEnvelope`, **plus** a container-level deserialization-error handler: since a
  malformed record fails inside the JSON deserializer itself (before any `KafkaMessageEnvelope`
  exists), the listener method body alone cannot observe it — the error handler intercepts the
  per-record deserialization exception at the container level, produces an
  `INVALID_DESERIALIZATION` outcome for that record, and lets the container continue polling
  (depends on: T011, T013)
- [ ] T040 [US3] Implement `MessageValidator` enforcing `contracts/kafka-message-contract.md`'s
  3-point validity rule (envelope fields present AND `payload.content` present and non-empty) for
  records that *did* deserialize successfully, in
  `consumer-service/src/main/java/com/shan/kafka/consumerservice/kafka/MessageValidator.java`
  (depends on: T011)
- [ ] T041 [US3] Implement `ProcessingOutcome` (`PROCESSED` / `INVALID_DESERIALIZATION` /
  `INVALID_VALIDATION`) determination and per-outcome logging in
  `consumer-service/src/main/java/com/shan/kafka/consumerservice/kafka/MessageProcessor.java` —
  `INVALID_DESERIALIZATION` outcomes originate from T039's container-level error handler (no
  envelope object exists), while `PROCESSED`/`INVALID_VALIDATION` originate from the listener's
  normal path after T040's validation runs on a successfully deserialized envelope (depends on:
  T039, T040)
- [ ] T042 [US3] Implement manual acknowledgment for every outcome — `PROCESSED`,
  `INVALID_DESERIALIZATION` (acknowledged from T039's error handler), and `INVALID_VALIDATION`
  (acknowledged from the normal listener path) are all acknowledged/committed once logged, per
  data-model.md's Acknowledgement rule, so no invalid message — deserialization failure or
  validation failure alike — can block subsequent valid ones (FR-014, SC-008). No DLQ or retry
  queue is introduced; a record that cannot be handled is skipped-and-acknowledged, not requeued
  (depends on: T041)
- [ ] T043 [US3] Implement `ConsumerRuntimeState` (`messagesConsumed`, `messagesRejected`,
  `lastError`) updated per `ProcessingOutcome` (depends on: T041)

**Checkpoint**: User Stories 1–3 all work independently; the end-to-end produce→consume pipeline
functions.

---

## Phase 6: User Story 4 - Observe System Health and Throughput (Priority: P4)

**Goal**: Both services expose observable status without requiring source-code inspection, and
correctly report through Kafka broker unavailability.

**Independent Test**: With either/both services running, query each service's status endpoint and
confirm it reflects reality (state, rate, counts, errors).

### Tests for User Story 4 (write first; must fail before implementation)

- [ ] T044 [P] [US4] Test: `GET /status` on `producer-service` returns `state`, `configuredRate`,
  `messagesProduced`, `lastSequenceNumber`, `lastError` per `contracts/producer-api.md` (FR-019,
  FR-020, SC-009) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/api/ProducerStatusTest.java`
- [ ] T045 [P] [US4] Test: `GET /status` on `consumer-service` returns `messagesConsumed`,
  `messagesRejected`, `lastError` per `contracts/consumer-status-api.md` (FR-021, SC-009) in
  `consumer-service/src/test/java/com/shan/kafka/consumerservice/api/ConsumerStatusTest.java`
- [ ] T046 [P] [US4] Kafka-unavailable test: rather than physically stopping/restarting the
  embedded broker mid-test (unreliable timing), substitute a deterministic test double for T012's
  injectable Kafka producer bean that is toggled to fail every send ("unavailable") and later
  toggled back to succeed ("restored"); assert that while failing, `producer-service`'s
  `GET /status` still returns `200 OK` with a well-defined `state` (never undefined/inconsistent)
  and a non-null `lastError`, and that `lastError` clears (or a subsequent successful send is
  observed) once the double is toggled back to succeeding (FR-023, SC-007) in
  `producer-service/src/test/java/com/shan/kafka/producerservice/kafka/BrokerUnavailableProducerTest.java`
  (depends on: T012)
- [ ] T047 [P] [US4] Kafka-unavailable test: substitute a deterministic test double for T013's
  injectable Kafka consumer/listener-container error path that is toggled to raise a connectivity
  error and later toggled back to normal operation, in place of physically stopping/restarting the
  embedded broker; assert `consumer-service` does not crash while the double is failing, surfaces
  the condition via `lastError`, and resumes consuming once the double is toggled back (FR-024) in
  `consumer-service/src/test/java/com/shan/kafka/consumerservice/kafka/BrokerUnavailableConsumerTest.java`
  (depends on: T013)

### Implementation for User Story 4

- [ ] T048 [P] [US4] Implement `GET /status` on `producer-service`'s
  `ProducerControlController`, served entirely from `ProducerLifecycle`'s in-memory state (no live
  broker check — SC-007, SC-009) per `contracts/producer-api.md` (depends on: T021, T025)
- [ ] T049 [P] [US4] Implement `GET /status` on a new `ConsumerStatusController`, served entirely
  from `ConsumerRuntimeState`'s in-memory state (no live broker check) per
  `contracts/consumer-status-api.md` in
  `consumer-service/src/main/java/com/shan/kafka/consumerservice/api/ConsumerStatusController.java`
  (depends on: T043)
- [ ] T050 [US4] Ensure a producer send failure (e.g., broker unavailable) sets `lastError` without
  moving `state` outside {STOPPED, RUNNING} (FR-023) in `ProducerLifecycle`/`ProductionTask`
  (depends on: T021, T023)
- [ ] T051 [US4] Ensure a consumer Kafka connectivity failure sets `lastError` and the consumer
  recovers once connectivity returns, without crashing (FR-024) in `ConsumerKafkaConfig`/the
  listener's error handling (depends on: T013, T039)

**Checkpoint**: All four user stories are independently functional.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Requirements that span the whole feature rather than one user story.

- [ ] T052 [P] Implement graceful shutdown on `producer-service`: stop issuing new messages and
  cancel the active `ProductionTask` on application shutdown, within a reasonable window (FR-025)
  in `ProducerLifecycle`
- [ ] T053 [P] Implement graceful shutdown on `consumer-service`: stop consuming and complete
  acknowledgment of in-flight work before process termination (FR-025) in the Kafka listener
  container configuration
- [ ] T054 [P] Test: `producer-service` stops producing new messages on a shutdown signal (FR-025)
  in `producer-service/src/test/java/com/shan/kafka/producerservice/lifecycle/GracefulShutdownTest.java`
- [ ] T055 [P] Test: `consumer-service` completes acknowledgment of in-flight work before shutdown
  completes (FR-025) in
  `consumer-service/src/test/java/com/shan/kafka/consumerservice/kafka/GracefulShutdownTest.java`
- [ ] T056 Run `quickstart.md`'s full validation guide (golden path + edge cases) manually against
  both running services to confirm the end-to-end feature works together
- [ ] T057 [P] Confirm `producer-service` builds and runs independently via
  `producer-service/mvnw.cmd clean install` with zero dependency on `consumer-service` (FR-001,
  constitution Principle IV)
- [ ] T058 [P] Confirm `consumer-service` builds and runs independently via
  `consumer-service/mvnw.cmd clean install` with zero dependency on `producer-service` (FR-001,
  constitution Principle IV)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately.
- **Foundational (Phase 2)**: Depends on Setup. Blocks all user stories.
- **User Story 1 (Phase 3)**: Depends on Foundational only.
- **User Story 2 (Phase 4)**: Depends on Foundational; builds on US1's `ProductionTask` (T023) —
  not independently buildable without US1's implementation existing, but independently *testable*
  once built (its tests exercise properties US1 doesn't assert).
- **User Story 3 (Phase 5)**: Depends on Foundational only — does not require `producer-service` to
  be running (FR-012), so it can be built and tested in parallel with US1/US2 by a different
  developer.
- **User Story 4 (Phase 6)**: Depends on Foundational; its producer-side tasks depend on US1
  (T021/T025) and its consumer-side tasks depend on US3 (T043).
- **Polish (Phase 7)**: Depends on all four user stories being complete.

### Within Each User Story

- Tests are written first and must fail before implementation begins (constitution Principle III).
- Lifecycle/config before the scheduling/production logic that depends on it.
- Production/consumption logic before the controller that exposes it.

### Parallel Opportunities

- All Setup tasks marked [P] (T001, T002, T004, T006) can run in parallel; T003/T005/T007 touch
  files other Setup tasks also touch or are inherently sequential (repo-root cleanup).
- All Foundational [P] tasks (T008–T011, T013) can run in parallel; T012 depends on T005 and T010.
- US1 and US3 can be staffed in parallel once Foundational is done (US3 doesn't need
  `producer-service` running). US2 and US4 depend on US1 (and US4 also on US3), so they trail.
- Within a story, all test tasks marked [P] can run in parallel (different files); most
  implementation tasks within a story are sequential because they build up the same class.

---

## Parallel Example: User Story 1

```bash
# Launch all US1 tests together (different files, no shared state):
Task: "Embedded-Kafka test: POST /startmsg from STOPPED in producer-service/src/test/.../api/StartMsgTest.java"
Task: "Embedded-Kafka test: POST /stopmsg from RUNNING in producer-service/src/test/.../api/StopMsgTest.java"
Task: "Idempotent /startmsg test in producer-service/src/test/.../lifecycle/StartIdempotencyTest.java"
Task: "Idempotent /stopmsg test in producer-service/src/test/.../lifecycle/StopIdempotencyTest.java"
Task: "Invalid rate test in producer-service/src/test/.../api/InvalidRateTest.java"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup.
2. Complete Phase 2: Foundational (blocks everything else).
3. Complete Phase 3: User Story 1.
4. **STOP and VALIDATE**: exercise start/stop, idempotency, concurrency, and the R2 race test
   independently against `producer-service` alone.

### Incremental Delivery

1. Setup + Foundational → foundation ready.
2. Add User Story 1 → validate independently (MVP: race-free lifecycle control).
3. Add User Story 2 → validate independently (rate accuracy + full message contract).
4. Add User Story 3 → validate independently (consumption, invalid-message handling).
5. Add User Story 4 → validate independently (observability, Kafka-unavailable behavior).
6. Polish (graceful shutdown, independent-build confirmation, full quickstart run).

### Parallel Team Strategy

With two developers: one takes `producer-service` (US1 → US2 → US4's producer half), the other
takes `consumer-service` (US3 → US4's consumer half) — the only thing they must agree on up front
is the Kafka message contract (T010/T011), which is identical by construction since both come from
`contracts/kafka-message-contract.md`.

---

## Notes

- [P] tasks touch different files and have no dependency on an incomplete task.
- Every test task must be written and observed failing before its corresponding implementation
  task is started (constitution Principle III, NON-NEGOTIABLE).
- No task in this list adds REST/RPC between the two services, a database, retry/DLQ
  infrastructure, a schema registry, exactly-once processing, authentication, or multi-instance
  consumer scaling — these remain out of scope per FR-002, FR-026, FR-027, FR-029 and the
  constitution.
- Commit after each task or logical group; stop at any checkpoint to validate a story
  independently.
