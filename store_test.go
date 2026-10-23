package gotelemetrypipeline

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TELEMETRY_DATABASE_URL")

	if dsn == "" {
		t.Skip("TELEMETRY_DATABASE_URL not set; skipping integration test")
	}

	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("error creating pool object: %v", err)
	}

	t.Cleanup(pool.Close)

	return pool
}

func TestDatabasePing(t *testing.T) {
	pool := newTestPool(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)

	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping pool: %v", err)
	}
}

func newTestEvent(t *testing.T) Event {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("failed to create uuid: %v", err)
	}

	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	ts := uint64(now.Unix())

	ev := Event{ID: id.String(), Timestamp: new(ts), Key: "input_traffic", Value: new(100.0)}

	return ev
}

func assertStoredEvent(ctx context.Context, t *testing.T, pool *pgxpool.Pool, want Event) {
	t.Helper()
	var (
		key       string
		value     float64
		eventTime time.Time
	)

	err := pool.QueryRow(ctx, "SELECT key,value,event_time FROM event WHERE id = $1", want.ID).Scan(&key, &value, &eventTime)
	if err != nil {
		t.Fatalf("failed to query event: %v", err)
	}

	if key != want.Key {
		t.Fatalf("key returned from database is different, key: %s, want: %s", key, want.Key)
	}

	if value != *want.Value {
		t.Fatalf("value returned from database is different, value: %f, want: %f", value, *want.Value)
	}

	if !eventTime.Equal(time.Unix(int64(*want.Timestamp), 0)) {
		t.Fatalf("eventTime returned from database is different, eventTime: %v, want: %v", eventTime, time.Unix(int64(*want.Timestamp), 0))
	}
}

func TestStoreInsertEvent(t *testing.T) {
	pool := newTestPool(t)
	store := NewStore(pool)

	t.Run("new event is inserted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		ev := newTestEvent(t)

		inserted, err := store.InsertEvent(ctx, ev)
		if err != nil {
			t.Fatalf("InsertEvent: err: %v", err)
		}

		if !inserted {
			t.Fatalf("first insert: inserted = false, want true")
		}

		assertStoredEvent(ctx, t, pool, ev)
	})

	t.Run("duplicate is reported, not failed", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		ev := newTestEvent(t)

		inserted, err := store.InsertEvent(ctx, ev)
		if err != nil {
			t.Fatalf("first InsertEvent: err: %v", err)
		}

		if !inserted {
			t.Fatalf("first insert: inserted = false, want true")
		}

		inserted, err = store.InsertEvent(ctx, ev)
		if err != nil {
			t.Fatalf("second InsertEvent: err: %v", err)
		}

		if inserted {
			t.Fatalf("second insert (duplicate): inserted = true, want false")
		}

		assertStoredEvent(ctx, t, pool, ev)
	})
}
