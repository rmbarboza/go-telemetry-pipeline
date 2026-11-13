# 0004. Consumer offset commits and error handling

## Status

Proposed. Relates to ADR 0001 and ADR 0002.

## Context

A `202` means the event is in Kafka (ADR 0001), so the consumer is the only
path to PostgreSQL: a record it skips is lost. Inserts are idempotent by `id`
(ADR 0002), so reprocessing costs work, not correctness.

## Decision

- Auto-commit is disabled. Offsets are committed once per poll batch, after
  every record in it is stored or skipped. A duplicate (`inserted == false`
  with a nil error) counts as stored; any non-nil error does not.
- With no committed offset, the group starts at `earliest`.
- Transient errors (database unreachable, timeout) block the whole consumer:
  the same record is retried with capped backoff and nothing is committed.
  Errors not known to be permanent are treated as transient.
- Permanent errors (the database rejects the record itself) are logged at
  error level with the `id` and skipped.
- Shutdown: stop polling, finish the current batch, commit with a deadline
  independent of the shutdown signal, then close the pool and the Kafka
  client.
- Rebalances wait until the current batch is committed
  (`BlockRebalanceOnPoll`), so partitions are never released with
  uncommitted work.

## Alternatives considered

- Commit per record: one broker round trip per event; idempotency already
  makes reprocessing a batch cheap.
- Skip transient errors: a database outage loses every event accepted during it.
- Retry permanent errors forever: one bad record stops its partition until
  retention deletes the records behind it.
- Pause only the failing partition: a database outage fails all partitions at
  once; per-partition pausing adds state for no gain.
- Commit in `OnPartitionsRevoked`: the callback runs while a batch may be in
  progress and would need progress per partition shared between goroutines.

## Consequences

- A crash reprocesses up to one batch.
- While blocked, lag grows and the API keeps returning `202`; `retention.ms`
  bounds how long the database can be down without losing events. Lag needs
  an alert.
- Skipped records are lost until a dead-letter topic exists; each one is a
  validation gap in the API (ADR 0002).
- A permanent error misclassified as transient blocks the consumer instead of
  losing data.
- While retrying a transient error, the consumer also holds up rebalances;
  past `RebalanceTimeout` the group removes it and its partitions are
  reprocessed elsewhere.
