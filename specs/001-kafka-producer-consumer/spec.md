# Feature Specification: Kafka Rate-Controlled Producer/Consumer Microservices

**Feature Branch**: `001-kafka-producer-consumer`

**Created**: 2026-09-20

**Status**: Draft

**Input**: User description: "Kafka Rate-Controlled Producer/Consumer Microservices — two independently deployable Spring Boot microservices (producer-service, consumer-service) communicating only through Apache Kafka. producer-service exposes POST /startmsg and POST /stopmsg to control rate-controlled message production (messages/second, configurable); consumer-service consumes, validates, processes, and acknowledges messages. Requirements cover lifecycle correctness under repeated/concurrent start-stop, message contract, rate tolerance, configuration externalization, reliability/error handling, and observability."

## Clarifications

### Session 2026-09-20

- Q: When the consumer "validates the message" (FR-013), does that validation cover only the envelope/metadata fields (ID, timestamp, producer identifier, sequence number, payload present), or must it also enforce a defined structure/schema on the payload's contents? → A: Validate envelope fields AND enforce a specific required payload schema (schema defined now: a required, non-empty `content` field).
- Q: Should the `/startmsg` and `/stopmsg` control endpoints require any form of caller authentication/authorization, or are they intentionally open/unauthenticated for this project? → A: No authentication/authorization required — endpoints are open, intended for trusted/local use only.
- Q: Is the 10%-tolerance-over-a-60-second-window rate-accuracy target (SC-004/FR-016) acceptable as the formal acceptance criterion? → A: Confirmed as-is — within 10% of target, measured over a 60-second window.
- Q: What order of magnitude should the system be designed to handle for the configurable production rate? → A: 10 messages/second (target sustained rate for this feature's scope).
- Q: Should `consumer-service` ever run as multiple concurrent instances sharing one consumer group (for parallel consumption), or is a single running instance the intended scope for this feature? → A: Single consumer-service instance only — multi-instance scaling is out of scope.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Control Message Production Lifecycle (Priority: P1)

An operator wants to start and stop a stream of Kafka messages on demand, and trust that the
system always ends up in exactly one well-defined state (producing or not), even if they click
start or stop more than once or by accident.

**Why this priority**: Start/stop control is the entry point to every other capability in this
feature. Without correct lifecycle control, rate control, message content, and consumption cannot
be meaningfully tested. It is also the most failure-prone area (duplicate loops, races), so getting
it right first is the highest-value, independently-shippable slice.

**Independent Test**: Can be fully tested by calling the start and stop APIs (including repeated
and concurrent calls) against producer-service alone and observing its reported lifecycle state and
whether messages appear on the configured Kafka topic — no consumer required.

**Acceptance Scenarios**:

1. **Given** the producer is STOPPED, **When** an operator calls the start API, **Then** the
   producer transitions to RUNNING and begins producing messages to the configured topic.
2. **Given** the producer is RUNNING, **When** an operator calls the stop API, **Then** the
   producer transitions to STOPPED and no further messages are produced.
3. **Given** the producer is already RUNNING, **When** the start API is called again, **Then** the
   call succeeds without error, the producer remains RUNNING, and no second concurrent production
   loop is created.
4. **Given** the producer is already STOPPED, **When** the stop API is called again, **Then** the
   call succeeds without error and the producer remains STOPPED.
5. **Given** multiple start requests arrive at the same time, **When** they are processed,
   **Then** exactly one production loop ends up active and the reported state is consistently
   RUNNING.

---

### User Story 2 - Produce Rate-Controlled, Traceable Messages (Priority: P2)

An operator wants production to happen at a configured rate (messages per second) and wants every
message to carry enough information to trace it back to its origin and position in the stream.

**Why this priority**: Rate control and message traceability are what make this more than a plain
on/off switch — they are the core "producer" behavior the feature exists to deliver, and they can
be verified independently of any consumer by inspecting messages on the topic directly.

**Independent Test**: Configure a target rate, start the producer, and over an observation window
measure the number of messages landing on the configured topic and inspect their contents — no
consumer required.

**Acceptance Scenarios**:

1. **Given** a configured target rate of N messages/second, **When** the producer runs for a
   sufficiently long observation window, **Then** the aggregate number of messages produced
   approximates N per second within a documented tolerance.
2. **Given** the producer is RUNNING, **When** any message is produced, **Then** it contains a
   unique message ID, a production timestamp, a producer/service identifier, a monotonically
   increasing sequence number, and an application payload with a required, non-empty `content`
   field.
3. **Given** the producer has been restarted after being stopped, **When** it produces new
   messages, **Then** each message ID remains unique and the sequence number behavior is
   well-defined (see Assumptions).
4. **Given** the Kafka topic name is changed via configuration, **When** the producer is started,
   **Then** messages are produced to the newly configured topic without a code change.

---

### User Story 3 - Consume and Process Produced Messages (Priority: P3)

An operator wants a separate consumer process to reliably pick up produced messages, validate
them, process/log them, and acknowledge them, so the end-to-end pipeline delivers observable value
rather than messages simply accumulating unread on a topic.

**Why this priority**: This completes the end-to-end pipeline but is only meaningful once messages
exist to consume (User Story 2). It is independently testable by pointing consumer-service at a
topic that already has messages on it (produced manually or by producer-service).

**Independent Test**: Publish messages to the configured topic (with producer-service running or
via any producer of matching messages), start consumer-service on its own, and verify via logs that
each message was received, validated, processed, and acknowledged.

**Acceptance Scenarios**:

1. **Given** valid messages exist on the configured topic, **When** consumer-service is running,
   **Then** every message is received, deserialized, validated, processed/logged, and
   acknowledged.
2. **Given** a message on the topic fails deserialization or fails validation, **When**
   consumer-service processes it, **Then** the failure is logged/handled and consumption of
   subsequent valid messages continues uninterrupted.
3. **Given** consumer-service is started with no producer-service running, **When** it starts,
   **Then** it starts cleanly and simply waits for messages to arrive.
4. **Given** consumer-service is stopped and restarted, **When** it resumes, **Then** it continues
   consuming according to the delivery semantics defined for the system (see Assumptions), without
   crashing.

---

### User Story 4 - Observe System Health and Throughput (Priority: P4)

An operator wants to determine, without reading source code, whether the producer is RUNNING or
STOPPED, what rate it's configured for, how many messages have been produced and consumed, and
whether any Kafka or processing errors have occurred.

**Why this priority**: Observability is what turns the first three stories into an operable system
rather than a black box; it is valuable on its own once any production/consumption is happening,
but it isn't required to prove the core lifecycle, rate, or processing behavior work.

**Independent Test**: With producer-service and/or consumer-service running, query each service's
observable status output and confirm it reflects reality (state, rate, counts, errors) without
inspecting internal code or state.

**Acceptance Scenarios**:

1. **Given** the producer is RUNNING at a configured rate, **When** an operator checks its
   observable status, **Then** the status reflects RUNNING, the configured rate, and a current
   produced-message count/sequence position.
2. **Given** the consumer has processed some messages, **When** an operator checks its observable
   status, **Then** the status reflects a current consumed-message count.
3. **Given** the Kafka broker becomes unavailable while a service is running, **When** an operator
   checks that service's observable status/logs, **Then** the error condition is visible rather
   than the service silently appearing healthy.

---

### Edge Cases

- What happens when `/startmsg` is called twice in rapid succession, including truly concurrent
  requests? Exactly one production loop must be active afterward, with a consistent RUNNING state.
- What happens when `/stopmsg` is called while the producer is already STOPPED? It succeeds safely
  with no error and no change in state.
- What happens when the configured production rate is zero, negative, or missing? The request/
  configuration is treated as invalid; production is not started (or is stopped) rather than
  producing at an undefined rate, and the condition is surfaced to the caller and/or observability.
- How does the consumer handle a message that fails to deserialize (e.g., corrupted payload)? It is
  logged/handled without crashing consumer-service, and subsequent valid messages continue to be
  processed.
- How does the consumer handle a message that deserializes successfully but fails validation (e.g.,
  an envelope field is missing, or the payload's required `content` field is missing/empty)? It is
  logged/handled as invalid without stopping consumption.
- What happens if the Kafka broker is unreachable when `/startmsg` is called? The producer's
  lifecycle state remains well-defined (it does not silently report RUNNING while unable to
  produce), and the error condition is observable.
- What happens if the Kafka broker becomes unreachable while the producer is already RUNNING? The
  producer does not crash or end up in an inconsistent/unknown lifecycle state; the disruption is
  observable.
- What happens when consumer-service is started before producer-service has ever produced anything?
  It starts cleanly and waits for messages to arrive.
- What happens during graceful shutdown of either service while messages are in flight? In-flight
  work completes or is safely abandoned consistent with the selected delivery semantics; neither
  service crashes or corrupts its reported lifecycle state.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST be composed of two independently deployable services,
  `producer-service` and `consumer-service`, each able to be deployed and operated without the
  other being present.
- **FR-002**: The two services MUST communicate exclusively through Apache Kafka; no direct
  synchronous communication (REST, RPC, shared database, or any other mechanism) between them is
  permitted.
- **FR-003**: `producer-service` MUST expose an HTTP endpoint (`POST /startmsg`) that starts
  continuous Kafka message production at the currently configured rate.
- **FR-004**: `producer-service` MUST expose an HTTP endpoint (`POST /stopmsg`) that stops any
  in-progress continuous message production.
- **FR-005**: `producer-service` MUST maintain an explicit lifecycle state of at least STOPPED and
  RUNNING, and MUST report the resulting state in the response of `/startmsg` and `/stopmsg` calls.
- **FR-006**: Invoking `/startmsg` while the producer is already RUNNING MUST NOT start a second
  concurrent production loop; the call MUST succeed idempotently and report the existing RUNNING
  state.
- **FR-007**: Invoking `/stopmsg` while the producer is already STOPPED MUST succeed idempotently,
  reporting the STOPPED state, without raising an error for "nothing to stop."
- **FR-008**: Start and stop operations MUST produce correct, consistent lifecycle state when
  invoked repeatedly or concurrently by multiple callers (e.g., simultaneous `/startmsg` calls MUST
  NOT result in more than one active production loop).
- **FR-009**: The message production rate MUST be configurable as a number of messages per second
  without requiring a code change or rebuild to adjust it.
- **FR-010**: Each produced message MUST include, at minimum: a unique message identifier, a
  production timestamp, a producer/service identifier, a monotonically increasing sequence number,
  and an application payload that itself contains a required, non-empty `content` field
  representing the arbitrary application data being transmitted.
- **FR-011**: The Kafka topic name used for production and consumption MUST be configurable rather
  than hardcoded.
- **FR-012**: `consumer-service` MUST be able to consume messages from the configured Kafka topic
  on startup without requiring `producer-service` to be running or reachable.
- **FR-013**: For every syntactically valid message consumed, `consumer-service` MUST deserialize
  it, validate its required envelope fields (message ID, timestamp, producer identifier, sequence
  number) and its payload's required `content` field, process/log it, and acknowledge it according
  to the delivery semantics selected for the system.
- **FR-014**: `consumer-service` MUST detect and handle messages that fail deserialization or fail
  required-field validation without crashing or permanently stalling consumption of subsequent
  messages.
- **FR-015**: `consumer-service` MUST emit sufficient log output to determine, after the fact, that
  a given message was received and successfully processed, or, for invalid messages, that it was
  detected and handled.
- **FR-016**: Over a sufficiently long measurement interval (not on a per-message basis), the
  aggregate production rate MUST approximate the configured messages-per-second target within a
  reasonable, documented tolerance.
- **FR-017**: The system MUST NOT require exact wall-clock timing guarantees for any individual
  message; acceptable variation includes OS/JVM scheduling jitter, Kafka client batching, network
  latency, and broker-side delay.
- **FR-018**: The following MUST be externally configurable per service/environment without code
  changes: Kafka bootstrap server(s), Kafka topic name, consumer group ID, and producer message
  rate.
- **FR-019**: `producer-service` MUST expose its current lifecycle state (RUNNING/STOPPED) and its
  currently configured production rate through an observable interface.
- **FR-020**: `producer-service` MUST expose a running count or last sequence number of messages
  produced since it started, sufficient to confirm production is occurring and at roughly what
  pace.
- **FR-021**: `consumer-service` MUST expose a running count of messages consumed/processed,
  sufficient to confirm consumption is occurring.
- **FR-022**: Both services MUST surface Kafka-related or processing errors (e.g., broker
  unavailability, deserialization failure) through logs or an observable interface rather than
  failing silently.
- **FR-023**: If the Kafka broker is unavailable when `/startmsg` is invoked or while production is
  RUNNING, the producer MUST NOT enter an inconsistent/undefined lifecycle state; it MUST remain in
  or return to a well-defined state and MUST surface the error condition via observability.
- **FR-024**: If the Kafka broker is unavailable at `consumer-service` startup or during
  consumption, the consumer MUST NOT crash outright; it MUST recover once connectivity is restored
  and MUST surface the error condition via observability.
- **FR-025**: Both services MUST support graceful shutdown: `producer-service` MUST stop issuing
  new messages and `consumer-service` MUST stop consuming and complete acknowledgment of in-flight
  work before process termination, within a reasonable shutdown window.
- **FR-026**: The system MUST NOT implement retry queues, dead-letter topics, schema registries,
  exactly-once delivery, or multiple consumer groups unless a requirement above cannot otherwise be
  satisfied.
- **FR-027**: The `/startmsg` and `/stopmsg` control endpoints MUST NOT require caller
  authentication or authorization; they are scoped to trusted/local use for this project and MUST
  remain reachable by any caller who can reach `producer-service`.
- **FR-028**: The system MUST correctly sustain a configured production rate of at least 10
  messages per second, within the tolerance defined in FR-016, using reasonable resource usage on
  typical developer hardware.
- **FR-029**: This feature's correctness guarantees (message processing, acknowledgment, and
  lifecycle correctness) apply to a single running instance of `consumer-service`; concurrent
  multi-instance consumption sharing one consumer group is out of scope.

### Key Entities

- **Producer Lifecycle State**: The current run state of `producer-service` (STOPPED or RUNNING),
  together with its configured rate and the point in time it last transitioned state. Drives
  whether the production loop is active and what `/startmsg`/`/stopmsg` report back.
- **Kafka Message (Envelope)**: The externally observable record contract carried on the Kafka
  topic — unique message ID, production timestamp, producer/service identifier, monotonically
  increasing sequence number, and an application payload (see Message Payload).
- **Message Payload**: The application-data portion of a Kafka message. MUST contain a required,
  non-empty `content` field representing the arbitrary application data being transmitted; this is
  the one payload-level field the consumer validates, in addition to the envelope fields above.
- **Production Rate Configuration**: The target messages-per-second value and the other externally
  configurable settings (bootstrap servers, topic, consumer group) that control system behavior
  without code changes.
- **Processing Outcome**: The result of `consumer-service` handling one consumed message —
  successfully processed, or detected as invalid (deserialization or validation failure) — used to
  drive logging and observability.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An operator can transition the producer from STOPPED to RUNNING and confirm, via
  observable status, that production has started within 5 seconds of calling the start API.
- **SC-002**: An operator can stop active production and confirm, via observable status, that no
  new messages are produced within 5 seconds of calling the stop API.
- **SC-003**: Repeating the start call while already RUNNING, or the stop call while already
  STOPPED, never returns an error and never results in more than one active production loop.
- **SC-004**: Over a 60-second observation window, the measured aggregate production rate is within
  10% of the configured messages-per-second target.
- **SC-005**: 100% of consumed valid messages contain all required fields (ID, timestamp, producer
  identifier, sequence number, payload with a non-empty `content` field), as confirmed by
  inspection/logging.
- **SC-006**: `consumer-service` and `producer-service` can each be started, stopped, and restarted
  independently of the other's state, without either service failing to start.
- **SC-007**: When the Kafka broker is temporarily unavailable, the producer's reported lifecycle
  state remains well-defined (STOPPED or RUNNING) at all times, never an inconsistent/unknown
  state, as confirmed via observable status.
- **SC-008**: 100% of malformed or invalid messages received by the consumer are logged/handled
  without stopping consumption of subsequent valid messages.
- **SC-009**: An operator can determine, using only each service's observable status/log output,
  whether the producer is RUNNING or STOPPED, its configured rate, and how many messages have been
  produced and consumed, without inspecting source code.
- **SC-010**: The system sustains a configured rate of 10 messages/second, within the tolerance
  defined in SC-004, for at least 5 consecutive minutes without resource exhaustion or lifecycle
  state corruption.

## Assumptions

- Repeated `/startmsg` calls while RUNNING, and repeated `/stopmsg` calls while STOPPED, are
  treated as **idempotent success** rather than explicit rejection, since this is simpler for
  callers to handle and is consistent with common REST lifecycle-control conventions.
- **At-least-once** delivery/acknowledgment is assumed as the default consumer semantics, since the
  feature description explicitly defers the exact Kafka delivery-semantics choice to implementation
  planning; exactly-once delivery is explicitly out of scope per the project constitution.
- Malformed or invalid messages are **logged and skipped**, not retried or routed to a dead-letter
  destination, consistent with the explicit exclusion of retry/dead-letter infrastructure unless a
  stated requirement cannot otherwise be met.
- A "sufficiently long measurement interval" for aggregate rate verification is confirmed as a
  60-second window with a 10% tolerance (see Clarifications, SC-004, FR-016) — long enough to
  average out scheduling and network jitter while remaining practical to test.
- Specific HTTP status codes for each API scenario (success, already-running, already-stopped,
  invalid configuration, internal failure) are intentionally not prescribed here, per the feature
  description; this is a planning-phase decision, constrained only by the requirement that
  responses expose the resulting producer state where applicable.
- Sequence numbers are scoped **per production run**, where a run begins at each STOPPED→RUNNING
  transition that actually starts production (i.e., each successful `/startmsg` call, including
  after a full service/process restart) and resets so the first successfully produced message in
  that run is `sequenceNumber = 1`, incrementing by 1 for each subsequent successfully produced
  message in the same run. An idempotent `/startmsg` no-op (already RUNNING) does not begin a new
  run and does not reset the sequence. Message uniqueness across runs and process restarts is still
  guaranteed via the unique message ID, independent of the sequence number.
- Both services are assumed to run as independently deployable Spring Boot applications (e.g.,
  separate processes/containers), consistent with the project constitution's service-boundary
  principle.
