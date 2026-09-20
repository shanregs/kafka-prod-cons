# Implementation Plan: Kafka Rate-Controlled Producer/Consumer Microservices

**Branch**: `001-kafka-producer-consumer` | **Date**: 2026-09-20 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-kafka-producer-consumer/spec.md`

**Note**: This template is filled in by the `/speckit-plan` command; its definition describes the execution workflow.

## Summary

Split the current single-module skeleton into two independently deployable Spring Boot services —
`producer-service` and `consumer-service` — that communicate only through Apache Kafka.
`producer-service` exposes `POST /startmsg` / `POST /stopmsg` to thread-safely start and stop a
rate-controlled production loop (configurable messages/second, no code-level maximum, validated to
correctly sustain at least 10 msg/s within ±10% over a 60s window per FR-028/SC-004/SC-010) and
produces an envelope message (`messageId`, `producedAt`, `producerId`, `sequenceNumber`,
`payload.content`) to a configurable topic. `consumer-service` consumes from that topic, validates
the envelope and payload, logs/processes each message, and acknowledges it at-least-once. Both
services expose observable status (state, rate, counts, errors) and externalize all Kafka/service
configuration. Technical approach: Spring Kafka + Spring Web on Java 25 / Spring Boot 4.1.1, a
single-threaded scheduled production loop guarded by a race-free install-then-activate lifecycle
protocol (research.md R2) so a production task can never start after the lifecycle has already
transitioned to STOPPED, embedded-broker-based test-first development, and no auth/DLQ/schema
registry/multi-instance scaling/distributed locks per the explicit scope boundaries in the spec and
constitution.

## Technical Context

**Language/Version**: Java 25

**Primary Dependencies**: Spring Boot 4.1.1 — `spring-boot-starter-kafka` (both services),
`spring-boot-starter-webmvc` (both services — `producer-service` for `/startmsg`/`/stopmsg`/status,
`consumer-service` for its own `GET /status`, `contracts/consumer-status-api.md`; corrected during
T004 implementation, this document originally scoped it to `producer-service` only),
`spring-boot-starter-actuator` (both services, for health/observability endpoints alongside the
custom status data — see research.md R5)

**Storage**: N/A — no persistent database; state is in-memory per service process, and the durable
record of "what happened" is the Kafka topic itself

**Testing**: Embedded Kafka broker via `spring-kafka-test` (`@EmbeddedKafka`), per research.md R1

**Target Platform**: JVM server processes (Linux or Windows), each service independently
deployable as its own executable Spring Boot JAR/container

**Project Type**: Multi-service backend — two independently deployable Spring Boot microservices,
no frontend

**Performance Goals**: Rate is configurable with no code-level maximum; the system MUST correctly
sustain **at least** 10 messages/second, within ±10% of the configured target measured over a
60-second window, as the acceptance floor (FR-016, FR-028, SC-004, SC-010) — not a cap on
throughput

**Constraints**: Thread-safe, idempotent `/startmsg`/`/stopmsg` with no duplicate production loops
under repeated/concurrent calls, and no production task may become active after the lifecycle has
transitioned to STOPPED (FR-006–FR-008, research.md R2); no direct synchronous coupling between the
two services (FR-002); no caller authentication on control endpoints (FR-027); graceful shutdown
within 10 seconds of a shutdown signal for both services (FR-025, SC-011); single
`consumer-service` instance only, no multi-instance/consumer-
group scaling (FR-029); no distributed locks, databases, retry/DLQ infrastructure, schema registry,
or exactly-once processing (FR-026, constitution Principle VI)

**Scale/Scope**: Two services, one Kafka topic, one consumer group, single instance of each
service, validated at a 10 msg/s acceptance floor with no artificial upper bound — a local/dev-scale
learning deployment, not a production-traffic system

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Gate | Initial Check | Post-Design Check |
|---|---|---|---|
| I. Incremental, Working Increments | Work is decomposable into small, independently runnable/verifiable slices | PASS — phased by the spec's P1–P4 user stories (lifecycle → rate/contract → consumption → observability); see Project Structure and tasks.md (next command) | PASS — data model and contracts are additive per story, no slice requires the others to compile |
| II. Explicit Kafka Configuration | Broker/topic/group/serde config declared explicitly, not implicit | PASS — `application.yaml` per service will explicitly set bootstrap servers, topic, group ID, rate (see research.md R6, data-model.md Configuration) | PASS — confirmed in data-model.md; no reliance on Spring Kafka auto-defaults |
| III. Test-First Kafka Behavior (NON-NEGOTIABLE) | Automated test written and failing before producer/consumer logic is implemented; infra choice deferred to planning | PASS (deferred) — infra choice is exactly what Phase 0 research.md R1 resolves | PASS — embedded-broker test-first approach documented in research.md and quickstart.md |
| IV. Independently Deployable Service Boundaries | Two services, Kafka-only communication, own build/package structure | PASS — Project Structure below defines two sibling Maven projects, each with its own `mvnw`, `pom.xml`, and base package; no shared runtime code | PASS — contracts/ confirms the only cross-service contract is the Kafka message envelope, nothing synchronous |
| V. Rate-Controlled, Thread-Safe Producer Control | REST start/stop, configurable rate (no artificial max), thread-safe with no duplicate loops and no post-STOPPED activation; mechanism decided in planning, not constitution | PASS (deferred) — concurrency/rate mechanism is resolved in Phase 0 research.md R2/R3 | PASS — research.md R2 documents the race-free install-then-activate protocol (a production task cannot become active once STOPPED is reported) and R3 confirms the rate mechanism imposes no code-level cap; data-model.md defines the lifecycle state transitions it must uphold |
| VI. Simplicity and Justified Dependencies | No fixed allowlist; each dependency justified here; no distributed locks, DBs, retry/DLQ, schema registry, exactly-once, or multi-instance scaling | PASS — every dependency above (webmvc, kafka, actuator, spring-kafka-test) is justified against a specific FR/SC in research.md; R2's fix uses only in-process atomic transitions, not a lock/queue/external coordinator, so it introduces no new dependency or scope | PASS — no new dependencies introduced during Phase 1 design; R2's race fix remains in-process and lock-free |
| Technology Stack Constraints (Governance) | Java 25, Spring Boot 4.1.1, Maven Wrapper per service, Kafka-only inter-service communication | PASS — see Technical Context and Project Structure | PASS |

No violations requiring justification; Complexity Tracking table is not needed.

## Project Structure

### Documentation (this feature)

```text
specs/001-kafka-producer-consumer/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md        # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
├── contracts/           # Phase 1 output (/speckit-plan command)
│   ├── producer-api.md
│   ├── consumer-status-api.md
│   └── kafka-message-contract.md
└── tasks.md             # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

### Source Code (repository root)

The existing single-module skeleton (`pom.xml`, `src/main/java/com/shan/kafka/kafkaprodcons/...`
at the repo root) is retired in favor of two sibling, independently buildable Maven projects, per
constitution Principle IV. Each carries its own Maven Wrapper so neither requires a local Maven
install (Technology Stack Constraints).

```text
producer-service/
├── mvnw, mvnw.cmd, .mvn/
├── pom.xml
├── src/main/java/com/shan/kafka/producerservice/
│   ├── ProducerServiceApplication.java
│   ├── api/            # REST controllers for /startmsg, /stopmsg, /status
│   ├── lifecycle/       # producer state machine + start/stop coordination
│   ├── kafka/           # Kafka producer configuration + message publishing
│   └── config/          # externalized configuration binding (rate, topic, bootstrap servers)
├── src/main/resources/application.yaml
└── src/test/java/com/shan/kafka/producerservice/
    ├── api/             # contract tests for /startmsg, /stopmsg, /status
    ├── lifecycle/       # concurrency/idempotency tests for start/stop
    └── kafka/           # embedded-broker tests asserting produced message contract + rate

consumer-service/
├── mvnw, mvnw.cmd, .mvn/
├── pom.xml
├── src/main/java/com/shan/kafka/consumerservice/
│   ├── ConsumerServiceApplication.java
│   ├── api/             # REST controller for /status (FR-021)
│   ├── kafka/           # @KafkaListener + deserialization/validation/ack handling
│   └── config/          # externalized configuration binding (topic, group ID, bootstrap servers)
├── src/main/resources/application.yaml
└── src/test/java/com/shan/kafka/consumerservice/
    ├── api/             # status endpoint test
    └── kafka/           # embedded-broker tests for valid/invalid message handling
```

**Structure Decision**: Two independently deployable Spring Boot projects at the repository root,
`producer-service/` and `consumer-service/`, each with its own Maven Wrapper, `pom.xml`, and base
package (`com.shan.kafka.producerservice`, `com.shan.kafka.consumerservice`). There is no shared
module or shared code between them — the only thing they share is the Kafka message contract
documented in `contracts/kafka-message-contract.md`. The current root-level `pom.xml` and
`src/` skeleton are removed as part of this feature's implementation (tasks.md, next command),
since their content (base package `com.shan.kafka.kafkaprodcons`, single combined app) is superseded
by the two-service split.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No violations — table intentionally omitted.
