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

// TestProcessBatch checks which records ProcessBatch returns for
// commit and how many times it calls the store.
func TestProcessBatch(t *testing.T) {
	recs := []*kgo.Record{
		{Offset: 0, Value: []byte(`{"id": "1", "timestamp": 123, "key": "input_traffic", "value": 100}`)},
		{Offset: 1, Value: []byte(`{"id": "2", "timestamp": 124, "key": "input_traffic", "value": 200}`)},
		{Offset: 2, Value: []byte(`{"id": "3", "timestamp": 125, "key": "input_traffic", "value": 300}`)},
	}

	type testCase struct {
		name      string
		store     *fakeStore
		want      []*kgo.Record
		wantCalls int
	}

	tests := []testCase{
		{
			name:      "all records inserted",
			store:     &fakeStore{inserted: true},
			want:      recs,
			wantCalls: len(recs),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ProcessBatch(context.Background(), tc.store, recs)
			if err != nil {
				t.Fatalf("ProcessBatch returned error: %v", err)
			}

			if len(got) != len(tc.want) {
				t.Fatalf("ProcessBatch returned %d records, want %d", len(got), len(tc.want))
			}

			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("record %d: got offset %d, want offset %d", i, got[i].Offset, tc.want[i].Offset)
				}
			}

			if tc.store.calls != tc.wantCalls {
				t.Fatalf("InsertEvent was called %d times, want %d", tc.store.calls, tc.wantCalls)
			}
		})
	}
}
