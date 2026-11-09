# 0003. Producer record key, publish deadline and topic provisioning

## Status

Proposed

Relates to ADR 0001 (ingestion acknowledgement boundary) and ADR 0002 (event
schema and idempotency).

## Context

ADR 0001 defines that the API returns `202 Accepted` only after Kafka
acknowledges the write, and `503` with `Retry-After` otherwise. Implementing
the producer requires three decisions that ADR 0001 left open.

**Record key.** The Kafka partitioner uses the record key to choose a
partition. Records with the same key always land in the same partition, which
preserves their relative order. Records with a `nil` key are assigned by the
sticky partitioner: a whole batch goes to one partition, and the next batch
goes to another. Telemetry events have no ordering requirement between them:
duplicates are neutralized by `id` (ADR 0002) and each row stores its own
`event_time`, so the consumer does not depend on arrival order.

**Publish deadline.** A publish can block while the broker is slow or
unreachable. A timeout is ambiguous: the broker may have written the record
even though the API never received the acknowledgement.

**Topic creation.** If the producer is allowed to create topics on first use,
the topic gets the broker defaults for partition count, replication factor and
`min.insync.replicas`. These are exactly the settings ADR 0001 depends on for
durability, and partition count also caps consumer parallelism.

## Decision

- **Records are published with a `nil` key.** The sticky partitioner then
  fills larger batches, which means fewer and larger produce requests.
- **Every publish runs under its own deadline**, derived from the request
  context and shorter than the request's own limits:

  ```
  publish deadline  <  server WriteTimeout  <  timeout recommended to clients
  ```

  This guarantees the API always has time to answer `503` itself, instead of
  the connection being cut with no response. The concrete values live in
  named constants and in the README, not in this ADR.
- **Topics are provisioned explicitly**, outside the API process (script,
  operations or infrastructure as code). The producer does not enable
  `AllowAutoTopicCreation`; `franz-go` does not request auto-creation by
  default. A missing topic surfaces as `UNKNOWN_TOPIC_OR_PARTITION` and a
  `503`.

## Alternatives considered

- **Key = `event.ID`.** Each UUIDv7 hashes to a different partition, so
  batches stay small and the hashing buys no useful ordering: no consumer
  needs order by event ID.
- **Key = `event.Key`** (metric name). Preserves per-metric order, which no
  consumer needs, and creates hot partitions when one metric is much more
  frequent than the others.
- **Use the HTTP request context directly, without its own deadline.** The
  publish would be bounded only by the client's patience. If the client gives
  up first, the API loses the chance to answer `503`.
- **Auto-create topics on first publish.** Convenient locally, but production
  topics would silently get the broker defaults, possibly with a replication
  factor or `min.insync.replicas` that violates ADR 0001.

## Consequences

- No ordering guarantee between events, even for the same metric. Any future
  consumer that needs order must be addressed by a new ADR.
- A publish that times out after the broker has already written the record
  returns `503`. The client retries with the same `id`, and the duplicate is
  absorbed by `ON CONFLICT (id) DO NOTHING` (ADR 0002). Correctness depends on
  clients reusing the `id` on retries.
- The HTTP server must enforce read and write timeouts so the deadline chain
  holds. At the time of this ADR the server has none; this is fixed in a
  separate change.
- Deployments need a topic provisioning step before the API starts accepting
  traffic.
- **Local development deviation:** the single-broker environment uses the
  topic `events` with 3 partitions, replication factor 1 and
  `min.insync.replicas=1`. This does not provide the durability ADR 0001
  requires and must not be used as a reference for production.
