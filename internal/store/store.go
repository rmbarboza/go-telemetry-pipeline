package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rmbarboza/go-telemetry-pipeline/internal/event"
)

// Store persists events in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	store := Store{
		pool: pool,
	}

	return &store
}

// InsertEvent inserts a validated event. It returns inserted = false and a
// nil error when an event with the same id already exists, so retries under
// at-least-once delivery are not treated as failures (ADR 0002).
func (s *Store) InsertEvent(ctx context.Context, e event.Event) (bool, error) {
	res, err := s.pool.Exec(ctx,
		"INSERT INTO event (id,key,value,event_time) VALUES ($1,$2,$3,$4) ON CONFLICT(id) DO NOTHING",
		e.ID, e.Key, *e.Value, time.Unix(int64(*e.Timestamp), 0))
	if err != nil {
		err = fmt.Errorf("InsertEvent %s: %w", e.ID, err)
		return false, err
	}

	if res.RowsAffected() == 0 {
		return false, nil
	}

	return true, nil
}
