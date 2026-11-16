package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rmbarboza/go-telemetry-pipeline/internal/event"
)

const validID = "01890123-4567-7890-a123-456789abcdef"

type fakePublisher struct {
	err   error
	calls int
}

func (f *fakePublisher) Publish(ctx context.Context, e event.Event) error {
	f.calls++
	return f.err
}

func TestHTTPRouteMetric(t *testing.T) {
	type testCase struct {
		name           string
		body           string
		method         string
		pub            *fakePublisher
		wantStatus     int
		wantRetryAfter bool
		wantCalls      int
	}

	now := time.Now()

	tests := []testCase{
		{
			name:       "json with value",
			body:       `{"id": "` + validID + `","timestamp": ` + strconv.FormatInt(now.Unix(), 10) + `, "key": "input_traffic", "value": 0}`,
			method:     "POST",
			pub:        &fakePublisher{err: nil},
			wantStatus: http.StatusAccepted,
			wantCalls:  1,
		},
		{
			name:       "json without Value",
			body:       `{"id": "` + validID + `","timestamp": ` + strconv.FormatInt(now.Unix(), 10) + `, "key": "input_traffic"}`,
			method:     "POST",
			pub:        &fakePublisher{err: nil},
			wantStatus: http.StatusBadRequest,
			wantCalls:  0,
		},
		{
			name:       "invalid method GET",
			method:     "GET",
			pub:        &fakePublisher{err: nil},
			wantStatus: http.StatusMethodNotAllowed,
			wantCalls:  0,
		},
		{
			name:       "json without timestamp",
			body:       `{"id": "` + validID + `","key": "input_traffic", "value": 0}`,
			method:     "POST",
			pub:        &fakePublisher{err: nil},
			wantStatus: http.StatusBadRequest,
			wantCalls:  0,
		},
		{
			name:       "json with timestamp 0",
			body:       `{"id": "` + validID + `","timestamp": 0, "key": "input_traffic", "value": 0}`,
			method:     "POST",
			pub:        &fakePublisher{err: nil},
			wantStatus: http.StatusBadRequest,
			wantCalls:  0,
		},
		{
			name:       "json without key",
			body:       `{"id": "` + validID + `","timestamp": ` + strconv.FormatInt(now.Unix(), 10) + `, "value": 0}`,
			method:     "POST",
			pub:        &fakePublisher{err: nil},
			wantStatus: http.StatusBadRequest,
			wantCalls:  0,
		},
		{
			name:       "json with key empty string",
			body:       `{"id": "` + validID + `","timestamp": ` + strconv.FormatInt(now.Unix(), 10) + `, "key": "", "value": 0}`,
			method:     "POST",
			pub:        &fakePublisher{err: nil},
			wantStatus: http.StatusBadRequest,
			wantCalls:  0,
		},
		{
			name:           "service unavailable",
			body:           `{"id": "` + validID + `","timestamp": ` + strconv.FormatInt(now.Unix(), 10) + `, "key": "input_traffic", "value": 0}`,
			method:         "POST",
			pub:            &fakePublisher{err: errors.New("broker down")},
			wantStatus:     http.StatusServiceUnavailable,
			wantRetryAfter: true,
			wantCalls:      1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(
				tc.method,
				"/metrics",
				strings.NewReader(tc.body),
			)
			req.Header.Set("Content-Type", "application/json")

			rec := httptest.NewRecorder()
			mux := NewMux(tc.pub)
			mux.ServeHTTP(rec, req)

			if tc.pub.calls != tc.wantCalls {
				t.Errorf("received calls: %d, want: %d", tc.pub.calls, tc.wantCalls)
			}

			if rec.Code != tc.wantStatus {
				t.Errorf("received status: %d, want: %d", rec.Code, tc.wantStatus)
			}

			if tc.wantRetryAfter && rec.Header().Get("Retry-After") == "" {
				t.Errorf("expected Retry-After header")
			}
		})
	}

}
