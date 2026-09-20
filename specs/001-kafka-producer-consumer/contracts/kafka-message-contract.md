# Contract: Kafka Message Envelope

This is the only contract between `producer-service` and `consumer-service` (FR-002, FR-010,
FR-013). Serialization format: JSON string value (research.md R4). Kafka message key: unset.

## Schema

```json
{
  "messageId": "3f1a9b2e-2c9b-4b8e-9f0a-1a2b3c4d5e6f",
  "producedAt": "2026-09-20T14:32:07.123Z",
  "producerId": "producer-service",
  "sequenceNumber": 42,
  "payload": {
    "content": "arbitrary application data"
  }
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | yes | Unique per message; unique across producer restarts |
| `producedAt` | string, ISO-8601 timestamp, always UTC | yes | Production time |
| `producerId` | string | yes | Identifies the producing service/instance |
| `sequenceNumber` | integer, ≥ 1 | yes | Starts at 1 for the first successfully produced message of a production run, +1 per subsequent successfully produced message in that run; resets at the next STOPPED→RUNNING transition (see data-model.md Sequencing rules) |
| `payload.content` | string | yes, non-empty | The application data; the one payload field the consumer validates |

## Consumer validation rules (FR-013, FR-014)

A message is **valid** only if:
1. It deserializes as the JSON shape above, AND
2. `messageId`, `producedAt`, `producerId`, and `sequenceNumber` are all present, AND
3. `payload.content` is present and non-empty.

Any other case (deserialization failure, or any envelope/payload rule above violated) is an
**invalid message**: it MUST be logged/handled (`ProcessingOutcome` = `INVALID_DESERIALIZATION` or
`INVALID_VALIDATION`, see data-model.md) without stopping consumption of subsequent messages
(FR-014, SC-008).

## Delivery semantics

At-least-once (Assumptions): `consumer-service` acknowledges a message only after an outcome has
been determined for it — see research.md and data-model.md `ProcessingOutcome`. Acknowledgement is
**not** conditional on the message being valid: `PROCESSED`, `INVALID_DESERIALIZATION`, and
`INVALID_VALIDATION` are all acknowledged/committed once logged (data-model.md "Acknowledgement
rule"). This is required by FR-014/SC-008 — an invalid message MUST NOT permanently stall
consumption, and leaving it unacknowledged under at-least-once semantics would cause it to be
redelivered indefinitely (a "poison pill"), blocking every message after it on that partition. On
restart, already-acknowledged messages (valid or invalid) are not redelivered; in-flight
(unacknowledged) messages at the time of a crash/restart — i.e., ones for which no outcome was yet
determined — may be redelivered and reprocessed, which is acceptable under at-least-once semantics
and does not violate any stated requirement (exactly-once is explicitly out of scope, FR-026).

This redelivery is Kafka's normal at-least-once consumer behavior (re-polling records after an
unacknowledged offset), not the application-level retry queue/retry infrastructure FR-026 excludes.
FR-026 bans building custom retry/DLQ mechanisms on top of message handling; it does not, and
cannot, disable the underlying at-least-once redelivery a Kafka consumer relies on by default.
