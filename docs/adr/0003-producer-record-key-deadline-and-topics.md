# 0003. Producer record key, publish deadline and topic provisioning

## Status

Accepted. Relates to ADR 0001 and ADR 0002.

## Context

Events have no ordering requirement: duplicates are removed by `id`
(ADR 0002) and each row stores its own `event_time`. A publish can time out
after the broker has already written the record. Topic settings (partitions,
replication factor, `min.insync.replicas`) are what ADR 0001 relies on for
durability.

## Decision

- Records are published with a `nil` key, so the sticky partitioner builds
  larger batches.
- Each publish has its own deadline, shorter than the request limits:
  `publish deadline < server WriteTimeout < client timeout`. Values live in
  named constants and in `docs/architecture.md`.
- Topics are provisioned explicitly; `AllowAutoTopicCreation` stays disabled.

## Alternatives considered

- Key = `event.ID`: spreads records across partitions, smaller batches, no
  useful ordering.
- Key = `event.Key`: per-metric order nobody needs, hot partitions.
- Auto-created topics: broker defaults may violate ADR 0001.

## Consequences

- No ordering between events, even for the same metric.
- A timeout after a successful write returns `503`; the client retry is
  absorbed by `ON CONFLICT (id) DO NOTHING` (ADR 0002).
- Local single broker deviates from ADR 0001: RF 1, `min.insync.replicas=1`.
