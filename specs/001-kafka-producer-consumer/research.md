# Phase 0 Research: Kafka Rate-Controlled Producer/Consumer Microservices

Each item below resolves one `NEEDS CLARIFICATION` / open technical decision from the Technical
Context, scoped to what the spec and constitution explicitly leave to implementation planning.

## R1. Test infrastructure for Kafka behavior testing

**Decision**: Use an embedded Kafka broker via `spring-kafka-test`'s `@EmbeddedKafka` for
producer/consumer behavioral tests in both services.

**Rationale**: The constitution (Principle III) requires test-first Kafka behavior but explicitly
leaves the mechanism to planning. An embedded, in-process broker starts fast (no Docker daemon
dependency), keeps the two services' test suites simple and hermetic, and is sufficient to prove
publish/consume/validate/acknowledge behavior at the throughput this feature must demonstrate (a
minimum of 10 msg/s per FR-028 — the design itself imposes no upper bound on the configurable
rate). It also runs cleanly in CI without extra infrastructure setup, in a
single-developer/learning-project context where the overhead of a container-based harness isn't
justified.

**Alternatives considered**:
- *Testcontainers (real Kafka in Docker)*: More production-realistic broker behavior, but adds a
  Docker dependency to every test run and slower startup — not justified for the correctness
  properties this feature needs to prove (lifecycle idempotency, message contract, basic rate
  behavior), per constitution Principle VI (simplicity, justified dependencies).
- *Mocking the Kafka client directly*: Rejected — it would not exercise real serialization,
  partitioning, or consumer-group behavior, undermining the "Test-First Kafka Behavior" principle's
  intent of testing against real Kafka semantics.

## R2. Producer lifecycle concurrency mechanism

**Decision**: A naive "compare-and-set STOPPED→RUNNING, then create/schedule the production task"
design leaves a race window: a concurrent `/stopmsg` can complete (finding nothing to cancel yet)
in the gap between `/startmsg` winning the state transition and the task actually being installed
and scheduled, leaving a production task that starts running *after* the lifecycle has already
reported STOPPED. To close that window, the lifecycle uses a single shared slot that holds either
"no active task" or exactly one task reference, combined with an **install-then-activate**
protocol:

1. `/startmsg` first constructs a new production task in an inert (not-yet-scheduled) form, then
   performs one compare-and-set of the shared slot from "no active task" to that new task
   reference. If the compare-and-set fails (a task is already installed), the new task is
   discarded and the call reports the existing RUNNING state (FR-006) — no second task is ever
   created.
2. Only after winning that compare-and-set does the caller ask the *installed* task to activate
   (begin its recurring rate-controlled execution). The task itself exposes a single one-time,
   mutually exclusive transition between "activate" and "cancel" — whichever of the two is
   requested first wins, and the other becomes a no-op.
3. `/stopmsg` atomically clears the shared slot (swap it to "no active task", capturing whatever
   task reference was there, if any) and immediately requests "cancel" on that captured task
   reference. Because activate/cancel on a given task are mutually exclusive:
   - If `/stopmsg`'s cancel reaches the task before `/startmsg`'s activate call, activate observes
     the task already cancelled and never schedules a single execution.
   - If activate reaches the task first, cancel stops the already-scheduled recurring execution.
   Either ordering leaves zero active production once STOPPED is the reported state — the specific
   race the naive design left open cannot occur, because the shared slot is cleared *before* the
   captured task is told to cancel, and the task's own activate/cancel transition is itself a
   single atomic decision independent of when `/startmsg`'s call happens to reach it.
4. This guarantees at most one active production loop under concurrent `/startmsg` calls (only one
   compare-and-set can win) and guarantees no production loop is ever active while the reported
   state is STOPPED (FR-008).

**Rationale**: The install-then-activate protocol keeps the same lock-free, single-process
simplicity as a plain compare-and-set guard (constitution Principle VI) while closing the
start/stop race a single flag-then-schedule approach leaves open. It adds one extra atomic
transition (on the task itself) rather than a lock, a queue, or any cross-process coordination.

**Alternatives considered**:
- *Naive CAS-then-schedule (original design)*: Rejected — race window described above.
- *Synchronized method wrapping create+schedule+cancel*: Would also close the race, but serializes
  start/stop behind a lock for the duration of scheduling calls; the install-then-activate approach
  achieves the same correctness without blocking.
- *External coordination (e.g., a distributed lock)*: Unnecessary and explicitly out of scope —
  this feature scopes to a single producer-service instance; no multi-instance coordination
  requirement exists (FR-029's single-instance scoping applies to the analogous producer case too).

## R3. Rate control mechanism

**Decision**: Implement the configured messages/second rate as a fixed-delay repeating task (period
= 1000ms / rate), each execution producing exactly one message, activated per the install-then-
activate protocol in R2 and cancelled the same way. The rate itself is read from configuration
(FR-009) with no code-level maximum — whatever positive rate is configured is what the period is
computed from; the design does not cap throughput at any specific number.

**Rationale**: This directly maps the externally observable requirement ("N messages per second,
aggregate, not exact per-message timing" — FR-016/FR-017) to the simplest scheduling shape, without
building a custom token-bucket or leaky-bucket rate limiter that the spec doesn't require. FR-028
requires the system to correctly sustain **at least** 10 msg/s — that figure is the acceptance
floor this design is validated against (SC-004, SC-010), not an upper bound the implementation
enforces; a higher configured rate simply yields a shorter fixed-delay period, with the same
mechanism applying unchanged.

**Alternatives considered**:
- *Custom token-bucket rate limiter*: More precise burst control, but unnecessary complexity given
  the explicitly relaxed timing tolerance (FR-017); fixed-delay scheduling meets the ±10%/60s
  tolerance (SC-004) at the required 10 msg/s floor under ordinary JVM/OS scheduling jitter, and
  scales the same way to higher configured rates without redesign.
- *Batch-then-sleep loop in a dedicated thread*: Equivalent in effect to fixed-delay scheduling but
  requires hand-rolled thread lifecycle management that a scheduled-task abstraction already
  provides more safely, and would need its own install-then-activate-style guard duplicated from
  R2.

## R4. Message envelope serialization

**Decision**: Serialize the Kafka message value as JSON (UTF-8 encoded string), with the envelope
fields (`messageId`, `producedAt`, `producerId`, `sequenceNumber`, `payload.content`) as top-level/
nested JSON fields per `contracts/kafka-message-contract.md`. The Kafka message key is left unset
(not required by any FR/SC).

**Rationale**: JSON is human-readable (helps a learning project's debuggability goal), is directly
supported by Spring Kafka's JSON (de)serializers, requires no schema registry (explicitly excluded,
FR-026), and is trivially inspectable with standard Kafka console tools during manual verification
(quickstart.md).

**Alternatives considered**:
- *Avro/Protobuf with schema registry*: Rejected outright by FR-026 (no schema registry).
- *Plain delimited string*: Would satisfy the contract but is harder to extend/validate than a
  structured JSON object, and Jackson (JSON) is already on the classpath transitively via
  `spring-boot-starter-webmvc`/`spring-boot-starter-kafka`, so it adds no new dependency.

## R5. Observability approach

**Decision**: Each service exposes a small custom status endpoint (`GET /status`, see
`contracts/producer-api.md` and `contracts/consumer-status-api.md`) returning the fields FR-019
through FR-022 require (state, configured rate, produced/consumed counts, last error), backed by
Spring Boot Actuator's health/liveness infrastructure for basic process health. Log output (SLF4J,
Spring Boot's default) covers per-message received/processed/invalid events (FR-015) and Kafka/
broker error conditions (FR-022–FR-024).

**Rationale**: A purpose-built status endpoint is the most direct way to satisfy the specific,
enumerated observability fields the spec requires; Actuator is added alongside it only for standard
process health/liveness, which is a normal, low-cost addition to any Spring Boot service and not a
speculative metrics-framework commitment (the spec explicitly says not to prescribe a metrics
framework — none is being prescribed here beyond basic health).

**Alternatives considered**:
- *Actuator custom metrics (Micrometer) exposed via `/actuator/metrics`*: Would work, but adds a
  metrics-framework dependency/learning surface the spec explicitly asked not to prescribe yet; a
  plain status DTO endpoint is simpler and sufficient for the stated FRs.
- *Log-only observability (no status endpoint)*: Rejected — FR-019/FR-021 require the state/counts
  to be queryable, not just discoverable by tailing logs.

## R6. Configuration externalization

**Decision**: Each service's `application.yaml` declares placeholders bound to environment
variables for: Kafka bootstrap servers, topic name, consumer group ID (consumer-service only), and
producer message rate (producer-service only) — e.g. `${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}`
style defaults for local development, overridable per environment without code changes (FR-018).

**Rationale**: Directly satisfies FR-018 and constitution Principle II (explicit configuration);
Spring Boot's standard `application.yaml` + environment-variable-override mechanism requires no
additional dependency and is the conventional approach already implied by the existing skeleton's
`application.yaml`.

**Alternatives considered**:
- *External config server (Spring Cloud Config)*: Unjustified additional infrastructure/dependency
  for a two-service local/dev-scale learning project.

## Outcome

All Technical Context unknowns are resolved. No `NEEDS CLARIFICATION` markers remain.
