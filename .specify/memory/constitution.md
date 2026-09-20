<!--
Sync Impact Report
- Version change: 1.0.0 → 2.0.0
- Modified principles:
  - III. Test-First for Producer/Consumer Behavior (NON-NEGOTIABLE) →
    III. Test-First Kafka Behavior (NON-NEGOTIABLE) — no longer mandates a specific test
    infrastructure (embedded broker); requires automated behavioral testing and defers the
    mechanism (embedded broker, Testcontainers, or otherwise) to the implementation plan.
  - IV. Package & Layout Consistency → IV. Independently Deployable Service Boundaries —
    redefined from a single-module package convention to a two-service architecture
    (producer-service, consumer-service) communicating solely via Kafka. This is a backward
    incompatible redefinition (single skeleton module → two independently deployable services),
    which is why this amendment is MAJOR.
  - V. Simplicity Over Framework Completeness → VI. Simplicity and Justified Dependencies —
    expanded to remove the constitution-level dependency allowlist; dependency choices are now
    justified in the implementation plan and reflected in project documentation instead.
- Added principles: V. Rate-Controlled, Thread-Safe Producer Control (new)
- Added sections: none (Technology Stack Constraints and Development Workflow retained, content
  revised)
- Removed sections: none
- Removed constraints: the enumerated Technology Stack Constraints dependency allowlist; the
  embedded-Kafka-broker test infrastructure mandate; the Java 21 requirement (corrected to Java 25)
- Follow-up TODOs: none — all placeholders resolved from user-supplied corrections.
-->

# kafka-prod-cons Constitution

## Core Principles

### I. Incremental, Working Increments
Every change MUST leave each affected service in a runnable, buildable state. Features are built
as small vertical slices — e.g., "producer publishes a plain string to a topic" is completed and
verified before adding custom serializers, error handling, rate control, or additional topics.
Large speculative changes that aren't independently verifiable are not permitted. Rationale: this
is a learning project; small, verified steps build real understanding of Kafka and Spring Boot
integration and surface mistakes immediately instead of burying them in a large diff.

### II. Explicit Kafka Configuration
Kafka broker addresses, topic names, consumer group IDs, and (de)serializer choices MUST be
declared explicitly in each service's configuration (`application.yaml` or a dedicated
`@Configuration` class), never left to implicit framework defaults or auto-detection. Rationale:
the purpose of this project is to learn how Kafka is wired into Spring Boot applications; hidden or
"magic" configuration defeats that learning goal even when it produces working code.

### III. Test-First Kafka Behavior (NON-NEGOTIABLE)
Before implementing producer or consumer logic, an automated test MUST be written that asserts the
expected publish/consume behavior, and it MUST fail before the implementation makes it pass. The
specific test infrastructure (embedded broker, Testcontainers, or another appropriate mechanism) is
NOT fixed by this constitution and MUST be selected and justified during implementation planning.
Rationale: Kafka's asynchronous, broker-dependent behavior is easy to believe works when it
silently doesn't; a red-green cycle against real Kafka behavior is the only reliable way to confirm
it, while the choice of harness is an implementation detail that may change as the project grows.

### IV. Independently Deployable Service Boundaries
The system consists of two independently deployable services — `producer-service` and
`consumer-service` — each with its own build, deployment, and package structure. The two services
MUST communicate only through Apache Kafka; no direct synchronous calls (REST, RPC, shared
database, etc.) between them are permitted. Each service maintains an internally consistent
package/module layout. Rationale: independent deployability and Kafka-only coupling are the point
of the exercise — a microservices boundary enforced through a broker, not through code sharing.

### V. Rate-Controlled, Thread-Safe Producer Control
`producer-service` MUST expose REST APIs to start and stop rate-controlled Kafka message
production, with the production rate configurable in messages per second. Start/stop control MUST
be thread-safe, and repeated invocations of the start endpoint MUST NOT create duplicate concurrent
producer loops — starting an already-running producer is a no-op (or explicit rejection), not a
second loop. The specific concurrency mechanism used to satisfy this is an implementation detail
and MUST be decided during implementation planning, not mandated here. Rationale: correct start/stop
semantics under repeated or concurrent calls is a core learning objective of this project and a
functional requirement of the target architecture, independent of how it's implemented.

### VI. Simplicity and Justified Dependencies
Only the Kafka/Spring features and dependencies needed for the current learning objective are
added — e.g., schema registries, multiple consumer groups, or retry/DLQ topologies are introduced
only when a specific concept calls for them, not preemptively. This constitution does not enumerate
an allowed dependency list; any dependency added to a service MUST be justified in the
implementation plan and reflected in that service's documentation. Rationale: as a personal roadmap
project, scope creep and unjustified dependencies obscure the specific concept being learned at
each stage, but a fixed allowlist would be too rigid to be worth maintaining here.

## Technology Stack Constraints

Each service targets Java `25` and Spring Boot `4.1.1`. Builds MUST use the Maven Wrapper
(`mvnw` / `mvnw.cmd`) checked into each service so builds are reproducible without a local Maven
install being a project requirement. Apache Kafka is the sole communication mechanism between
`producer-service` and `consumer-service`. Beyond these constraints, this document does not
enumerate specific dependencies or test-infrastructure choices — those are selected and justified
during implementation planning.

## Development Workflow

Build and test each service with its own Maven Wrapper (`mvnw.cmd clean install` on Windows) before
considering a change complete. Run a single test class with `mvnw.cmd test -Dtest=<ClassName>` and
a single method with `mvnw.cmd test -Dtest=<ClassName>#<methodName>`. This is a solo learning
repository with no PR review gate, but every commit MUST build and pass its service's test suite
before being considered done.

## Governance

This constitution governs development practices for the kafka-prod-cons learning project and
supersedes ad-hoc practices where they conflict. Amendments are made by editing
`.specify/memory/constitution.md` directly, recording a Sync Impact Report, and bumping the
version:
- MAJOR: backward-incompatible removal or redefinition of a principle or governance rule.
- MINOR: a new principle or section is added, or existing guidance is materially expanded.
- PATCH: wording clarifications, typo fixes, or non-semantic refinements.

`CLAUDE.md` holds day-to-day operational and architecture guidance for working in this repo; this
document governs the non-negotiable rules that guidance must respect. Any change to CLAUDE.md that
would contradict a principle here requires amending this constitution in the same change.
Implementation-level mechanisms (e.g., specific concurrency primitives, scheduling APIs, or test
harnesses) belong in implementation plans, not in this constitution.

**Version**: 2.0.0 | **Ratified**: 2026-09-20 | **Last Amended**: 2026-09-20
