# Contract: producer-service REST API

No authentication/authorization (FR-027). All endpoints return JSON. Exact HTTP status codes are an
implementation detail left open by the spec (Assumptions); the codes below are this plan's chosen
values, satisfying "responses expose the resulting producer state where appropriate."

FR-027 literally names only `/startmsg` and `/stopmsg`; `GET /status` below is given the same
no-auth, trusted/local-use posture by extension, on the same rationale, not because FR-027 itself
names it (mirroring the equivalent note in `consumer-status-api.md`).

## POST /startmsg

Starts rate-controlled production if STOPPED; idempotent no-op if already RUNNING (FR-006).

**Request**: no body required.

**Response** `200 OK` (both the "started" and "already running" cases — idempotent success,
Clarifications):
```json
{
  "state": "RUNNING",
  "configuredRate": 10
}
```

**Response** `409 Conflict` (invalid configuration — e.g., configured rate is zero, negative, or
missing; Edge Cases):
```json
{
  "state": "STOPPED",
  "error": "invalid configured rate: must be > 0"
}
```

**Kafka broker unreachable at call time**: this is not a distinct error response. Per FR-023, an
unreachable broker MUST NOT prevent the lifecycle from reaching a well-defined state, so a
`/startmsg` call with a valid (positive) configured rate still transitions to RUNNING and returns
`200 OK` as above — the production task activates regardless of broker reachability. Any resulting
send failures are surfaced asynchronously via `lastError` on `GET /status`, never by blocking or
failing the `/startmsg` call itself.

## POST /stopmsg

Stops production if RUNNING; idempotent no-op if already STOPPED (FR-007).

**Request**: no body required.

**Response** `200 OK` (both the "stopped" and "already stopped" cases):
```json
{
  "state": "STOPPED"
}
```

## GET /status

Observability endpoint (FR-019, FR-020, FR-022, FR-023).

**Response** `200 OK`:
```json
{
  "state": "RUNNING",
  "configuredRate": 10,
  "messagesProduced": 1523,
  "lastSequenceNumber": 1523,
  "lastError": null
}
```

`lastError` is a non-null string describing the most recent Kafka/production error (e.g., broker
unavailable) when one has occurred, without changing `state` to an undefined value (FR-023). This
endpoint is served entirely from `producer-service`'s in-memory lifecycle state (data-model.md); it
never performs a live Kafka connectivity check itself and therefore cannot fail or block because
the broker is unreachable — that condition is always reported via `lastError`, not via this
endpoint failing.

## Concurrency/idempotency contract (applies to both control endpoints)

- Two concurrent `POST /startmsg` calls MUST result in exactly one active production loop and both
  calls receiving `200 OK` with `state: "RUNNING"` (FR-008, SC-003).
- Two concurrent `POST /stopmsg` calls MUST both receive `200 OK` with `state: "STOPPED"`, and the
  production loop MUST be cancelled exactly once (no double-cancellation errors).
- A `POST /startmsg` racing a `POST /stopmsg` MUST resolve to one consistent, well-defined final
  state — never a state where the API reports RUNNING but no loop is active, or vice versa.
