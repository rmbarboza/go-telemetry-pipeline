package consumer

import (
	"context"
	"testing"

	"github.com/rmbarboza/go-telemetry-pipeline/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeStore struct {
	// Set by the test: what the fake should do.
	inserted   bool  // returned by calls that do not fail
	err        error // returned by the failing call
	failOnCall int   // 1-based number of the call that fails; 0 = never fails

	// Set by the fake: what happened.
	calls int // number of times InsertEvent was called
}

func (f *fakeStore) InsertEvent(ctx context.Context, e event.Event) (bool, error) {
	f.calls++
	if f.calls == f.failOnCall {
		return false, f.err
	}
	return f.inserted, nil
}

// TestProcessBatch checks that every record is returned for commit
// when all of them decode and the store accepts them.
func TestProcessBatch(t *testing.T) {
	store := &fakeStore{}
	recs := []*kgo.Record{
		{Offset: 0, Value: []byte(`{"id": "1", "timestamp": 123, "key": "input_traffic", "value": 100}`)},
		{Offset: 1, Value: []byte(`{"id": "2", "timestamp": 124, "key": "input_traffic", "value": 200}`)},
		{Offset: 2, Value: []byte(`{"id": "3", "timestamp": 125, "key": "input_traffic", "value": 300}`)},
	}

	got, err := ProcessBatch(context.Background(), store, recs)
	if err != nil {
		t.Fatalf("ProcessBatch returned error: %v", err)
	}

	if len(got) != len(recs) {
		t.Fatalf("ProcessBatch returned %d records, want %d", len(got), len(recs))
	}

	for i := range recs {
		if got[i] != recs[i] {
			t.Errorf("record %d: got offset %d, want offset %d", i, got[i].Offset, recs[i].Offset)
		}
	}

	if store.calls != len(recs) {
		t.Fatalf("InsertEvent was called %d times, want %d", store.calls, len(recs))
	}
}
