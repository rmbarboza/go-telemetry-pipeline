package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/rmbarboza/go-telemetry-pipeline/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EventStore persists events consumed from Kafka. InsertEvent reports
// inserted == false with a nil error when the event already exists;
// ProcessBatch treats that as processed (ADR 0004).
type EventStore interface {
	InsertEvent(ctx context.Context, e event.Event) (bool, error)
}

// ProcessBatch handles recs in order and returns the ones that were
// stored or skipped, so the caller commits only those (ADR 0004).
// On the first error it returns the records processed before the
// failing one, together with the error.
func ProcessBatch(ctx context.Context, s EventStore, recs []*kgo.Record) ([]*kgo.Record, error) {
	processed := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		slog.Info("event consumed",
			"partition", r.Partition,
			"offset", r.Offset,
			"value", string(r.Value),
		)

		var e event.Event
		err := json.Unmarshal(r.Value, &e)
		if err != nil {
			// TODO: invalid JSON is permanent: log and skip it instead of
			// failing the batch, with its own test case, before the PR.
			// A record without timestamp or value makes Store.InsertEvent
			// panic: decide whether ProcessBatch validates first.
			return processed, fmt.Errorf("decode record partition %d offset %d: %w", r.Partition, r.Offset, err)
		}

		inserted, err := s.InsertEvent(ctx, e)
		if err != nil {
			return processed, fmt.Errorf("store record partition %d offset %d: %w", r.Partition, r.Offset, err)
		}

		if !inserted {
			slog.Info("event already exists, skipping", "id", e.ID)
		}

		processed = append(processed, r)
	}
	return processed, nil
}
