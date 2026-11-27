# 0005. Consumer handling of malformed records

## Status

Accepted. Relates to ADR 0002 and ADR 0004.

## Context

ADR 0004 skips a record only when the database rejects it. A record whose
value does not decode into an `Event`, or whose event breaks the rules of
ADR 0002 (for example, no `timestamp` or `value`), fails before any insert
or depends on the database to catch it. The API validates and re-encodes
every event it publishes, so these records come only from another producer
writing to the topic. Retrying them never succeeds.

## Decision

- The consumer validates every decoded event with the API's event rules,
  except the acceptance window on `timestamp`. The window compares the event
  with the time it is received, so it is an API admission rule, checked only
  at ingestion; the other rules hold for an event at any time.
- A record that does not decode or fails validation is permanent: logged at
  error level with the partition and offset (and the `id`, when decoded) and
  skipped, as in ADR 0004.

## Alternatives considered

- Check only that `timestamp` and `value` are present: a second copy of the
  rules, and the store's precondition (a validated event) still unmet.
- Apply the window in the consumer: valid events reprocessed after a long lag
  would be dropped.
- Use the record timestamp as the time of receipt: depends on how producers
  set it, and an event at the window edge can fail milliseconds after the
  API accepted it.
- Send missing fields to the database as `NULL` (`23502`): the store would
  accept unvalidated events.
- Fail the batch: one malformed record stops every partition until an
  operator moves the group's offset past it.

## Consequences

- Skipped records are lost until a dead-letter topic exists. Each one points
  to a producer bypassing the API, not to a validation gap in it.
- Write access to the topic is a trust boundary: the window and the API's
  admission checks do not cover other producers.
- Event rules and the admission window are checked separately; a new entry
  point that skips the window accepts events of any age.
