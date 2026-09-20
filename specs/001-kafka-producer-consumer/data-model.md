# Data Model: Kafka Rate-Controlled Producer/Consumer Microservices

Derived from the feature spec's Key Entities, the Clarifications session, and research.md
decisions. This is a conceptual/data-shape model, not a class/schema implementation — field types
are described generically, not as Java types.

## ProducerLifecycleState (producer-service, in-memory)

Represents the current run state of `producer-service`.

| Field | Type | Notes |
|---|---|---|
| `state` | enum: `STOPPED` \| `RUNNING` | Initial value on startup: `STOPPED` (FR-005) |
| `configuredRate` | positive number (messages/second) | Sourced from configuration (R6); validated > 0 (Edge Cases: invalid rate) |
| `startedAt` | timestamp, nullable | Set on STOPPED→RUNNING transition; cleared (or retained as "last started at") on stop — exposed for observability, not a correctness requirement |
| `messagesProduced` | non-negative integer counter | Incremented per successfully produced message; reset to 0 on each STOPPED→RUNNING transition (Assumptions: sequence numbers scoped per run) |
| `lastSequenceNumber` | non-negative integer | Mirrors `messagesProduced` for the current run; exposed via status (FR-020). `0` specifically means no message has been successfully produced yet in the current run — distinct from the per-message `sequenceNumber` field on an actual produced message, which is always ≥1 |
| `lastError` | string, nullable | Last observed Kafka/production error message, if any (FR-022, FR-023); cleared on next successful production cycle |

`GET /status` (see `contracts/producer-api.md`) is served entirely from this in-memory state; it
never performs a live Kafka connectivity check and therefore cannot itself fail or block because
the broker is unreachable — broker errors are surfaced only through `lastError`, never by making
the status read fail (SC-007, SC-009).

**State transitions** (FR-005–FR-008, R2):

```
STOPPED --(/startmsg, wins install)--> RUNNING   [installs + activates one production task]
RUNNING --(/startmsg)--> RUNNING                 [idempotent no-op, reports RUNNING]
RUNNING --(/stopmsg, wins clear)--> STOPPED       [clears the slot + cancels the production task]
STOPPED --(/stopmsg)--> STOPPED                   [idempotent no-op, reports STOPPED]
```

Only the caller that wins the compare-and-set install (on `/startmsg`) or the clear (on
`/stopmsg`) actually activates or cancels the one shared production task, per R2's install-then-
activate protocol; all other concurrent callers observe the resulting state without side effects.
Because activation and cancellation on a given task are themselves a single, mutually exclusive
transition, no production task can become active once `state` has been reported as STOPPED — the
race a plain "flag, then schedule" design would allow is closed at the task level, not just the
state-flag level.

**Validation rules**:
- A start request when `configuredRate` is missing, zero, or negative MUST NOT transition to
  RUNNING; it is treated as invalid configuration (Edge Cases) and `lastError` is set accordingly.

## KafkaMessageEnvelope (wire contract, both services)

The externally observable record contract carried on the Kafka topic (FR-010; see
`contracts/kafka-message-contract.md` for the concrete JSON shape).

| Field | Type | Notes |
|---|---|---|
| `messageId` | string, unique | Unique per message, unique across producer restarts (Assumptions) |
| `producedAt` | timestamp (ISO-8601, always UTC) | Set at production time |
| `producerId` | string | Identifies the producing service/instance (FR-010) |
| `sequenceNumber` | integer, ≥ 1, monotonically increasing per production run | Starts at 1 for the first successfully produced message of the run, +1 per subsequent successfully produced message; reset at the next STOPPED→RUNNING transition (Assumptions) |
| `payload` | `MessagePayload` | See below |

**Sequencing rules**:
- A run's first successfully produced message MUST have `sequenceNumber = 1`; each subsequent
  successfully produced message in the same run MUST have `sequenceNumber` exactly one greater than
  the previous successfully produced message's.
- A production attempt that fails before being successfully sent (e.g., broker unavailable) MUST
  NOT consume a sequence number — only successful productions advance the sequence, so the sequence
  stays contiguous (no gaps) within a run.
- An idempotent `/startmsg` no-op (already RUNNING) does not start a new run and MUST NOT reset the
  sequence.
- **Accepted rare exception**: if a send's acknowledgment times out ambiguously (the broker may
  have actually received it despite the timeout), the next attempt reuses that same sequence number
  under a new `messageId` rather than skipping ahead (see T031). This can rarely leave two distinct
  messages sharing one `sequenceNumber` if the "timed-out" send had, in fact, succeeded. This is an
  accepted tradeoff (spec.md Assumptions, Clarifications), not a defect, and is intentionally not
  closed via retries, a dead-letter mechanism, or an idempotent/exactly-once producer (FR-026).

## MessagePayload

| Field | Type | Notes |
|---|---|---|
| `content` | string, required, non-empty | The one payload-level field validated by the consumer (Clarifications Q1, FR-010, FR-013) |

## ProductionRateConfiguration (producer-service, externalized)

Not a runtime domain object with behavior — the set of externally configurable values (FR-018,
R6) that determine producer behavior.

| Field | Source | Notes |
|---|---|---|
| `kafkaBootstrapServers` | env/config | Required, no hardcoded default in production use |
| `kafkaTopic` | env/config | Required (FR-011) |
| `ratePerSecond` | env/config | Required, must be > 0 (see ProducerLifecycleState validation) |

## ConsumerConfiguration (consumer-service, externalized)

| Field | Source | Notes |
|---|---|---|
| `kafkaBootstrapServers` | env/config | Required |
| `kafkaTopic` | env/config | Must match the producer's configured topic (FR-011) |
| `consumerGroupId` | env/config | Required (FR-018); single-instance scope only (FR-029) |

## ProcessingOutcome (consumer-service, per consumed message)

Drives logging/observability (FR-015, FR-021); not persisted beyond log output and running
counters.

| Field | Type | Notes |
|---|---|---|
| `messageId` | string, nullable | Null if the failure occurred before the envelope could be parsed at all |
| `outcome` | enum: `PROCESSED` \| `INVALID_DESERIALIZATION` \| `INVALID_VALIDATION` | `INVALID_VALIDATION` covers both a missing/invalid envelope field and a missing/empty `payload.content` (Edge Cases) |
| `detail` | string | Human-readable reason, logged; included in status `lastError` when applicable |
| `processedAt` | timestamp | When the outcome was determined |

**Acknowledgement rule** (FR-014, SC-008; see also `contracts/kafka-message-contract.md` Delivery
semantics): the Kafka offset is acknowledged/committed for **every** outcome above — `PROCESSED`,
`INVALID_DESERIALIZATION`, and `INVALID_VALIDATION` alike. An invalid message is logged/counted as
rejected and then acknowledged like any other, so it is never redelivered and can never block
consumption of the messages after it. Only a broker/connectivity failure (not a message-content
failure) can legitimately leave a message unacknowledged for at-least-once redelivery.

## ConsumerRuntimeState (consumer-service, in-memory)

| Field | Type | Notes |
|---|---|---|
| `messagesConsumed` | non-negative integer counter | Count of `PROCESSED` outcomes only (FR-021) |
| `messagesRejected` | non-negative integer counter | Count of `INVALID_*` outcomes, for observability |
| `lastError` | string, nullable | Last observed error (deserialization failure, validation failure, or broker connectivity issue — FR-022, FR-024) |
