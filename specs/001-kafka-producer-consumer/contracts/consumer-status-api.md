# Contract: consumer-service REST API

No authentication/authorization, consistent with producer-service's control endpoints (FR-027 is
scoped to producer-service, but the same trusted/local-use posture applies here since no
requirement introduces auth for consumer-service either).

## GET /status

Observability endpoint (FR-021, FR-022, FR-024).

**Response** `200 OK`:
```json
{
  "messagesConsumed": 1498,
  "messagesRejected": 3,
  "lastError": null
}
```

`lastError` is a non-null string describing the most recent deserialization/validation failure or
broker connectivity issue when one has occurred (FR-022, FR-024), without the endpoint itself
failing. This endpoint is served entirely from `consumer-service`'s in-memory runtime state
(data-model.md `ConsumerRuntimeState`); it never performs a live Kafka connectivity check itself,
so it cannot fail or block because the broker is unreachable — that condition is always reported
via `lastError`.

`messagesRejected` counts messages that were acknowledged/committed as `INVALID_DESERIALIZATION` or
`INVALID_VALIDATION` (see data-model.md `ProcessingOutcome` and the Acknowledgement rule) — they
are handled and skipped, not left pending, so they never block `messagesConsumed` from advancing on
subsequent valid messages (FR-014, SC-008).

`consumer-service` exposes no start/stop control — it runs continuously once started (User Story 3,
Independent Test), consistent with the spec defining lifecycle control only for the producer.
