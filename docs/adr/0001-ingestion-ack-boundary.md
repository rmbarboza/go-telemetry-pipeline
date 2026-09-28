# 0001. Ingestion acknowledgement boundary

## Status

Accepted

## Context

Clients send metric events to `POST /metrics`. Events will be published to
Kafka and later written to PostgreSQL by a separate consumer.

A client needs an unambiguous meaning for a success response: whether it may
discard the event or must retry. The API process can crash at any point, and
Kafka can be unavailable or slow. The client cannot distinguish a lost request
from a lost response, so it will retry on any failure or timeout.

## Decision

The API returns `202 Accepted` only after Kafka acknowledges the write.

- The producer uses `acks=all`; the topic uses `min.insync.replicas=2`.
- `202` guarantees the event is durably replicated in Kafka. It does not
  guarantee the event is stored in PostgreSQL or visible to queries.
- If Kafka is unavailable or does not acknowledge within the produce timeout,
  the API returns `503 Service Unavailable` with a `Retry-After` header. The
  event is not accepted and the client must retry.
- Validation failures still return `400` without contacting Kafka.
- Delivery is at-least-once. Duplicates are expected and will be handled
  downstream (see Consequences).

## Alternatives considered

- **Acknowledge after validation, publish in the background.** Lowest latency,
  but events are lost silently if the process crashes or Kafka is down: the
  client received success and never retries.
- **Acknowledge after the PostgreSQL write.** Strongest guarantee, but the
  request would wait for the whole asynchronous pipeline, adding consumer lag to
  latency and coupling ingestion availability to both Kafka and PostgreSQL.
  It still produces duplicates on crash-before-response.

## Consequences

- A success response means the event will not be lost unless multiple Kafka
  brokers fail.
- Response latency includes replication to in-sync replicas.
- Ingestion availability depends on Kafka; the API degrades to `503` instead of
  crashing, and recovers when Kafka returns.
- A crash between the Kafka acknowledgement and the HTTP response causes a
  client retry and a duplicate event. Deduplication requires an idempotency key
  in the event contract and a unique constraint in PostgreSQL; this will be
  decided in a later ADR.
- The current handler returns `200` after validation only. It must change to
  `202`/`503` when Kafka ingestion is implemented.