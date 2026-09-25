package gotelemetrypipeline

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeWithSuccess(t *testing.T) {
	testjson := `{"timestamp": 1790085318, "key": "input_traffic", "value": 100.5}`

	wantTs := uint64(1790085318)
	wantKey := "input_traffic"
	wantVal := 100.5

	metrict := Event{}
	if err := json.Unmarshal([]byte(testjson), &metrict); err != nil {
		t.Fatalf("error decoding json; err: %v", err)
	}

	if metrict.Timestamp == nil {
		t.Fatal("missing timestamp in received json")
	}

	if *metrict.Timestamp != wantTs {
		t.Fatalf("invalid value for timestamp: %v, want : %v", metrict.Timestamp, wantTs)
	}

	if metrict.Key != wantKey {
		t.Fatalf("invalid value for Key: %v, want : %v", metrict.Key, wantKey)
	}

	if metrict.Value == nil {
		t.Fatalf("invalid value nil for Value, want : %v", wantVal)
	}

	if *metrict.Value != wantVal {
		t.Fatalf("invalid value for Value: %v, want : %v", metrict.Value, wantVal)
	}
}

func TestDecodeWithFail(t *testing.T) {
	testjson := `{"timestamp": 1790085318, "key": "input_traffic", "value": "100.5"}`

	metrict := Event{}
	if err := json.Unmarshal([]byte(testjson), &metrict); err == nil {
		t.Fatal("expected an error for value encoded as string")
	}
}

func TestDecodeWithoutValue(t *testing.T) {
	testjson := `{"timestamp": 1790085318, "key": "input_traffic"}`

	metrict := Event{}
	if err := json.Unmarshal([]byte(testjson), &metrict); err != nil {
		t.Fatalf("error decoding json; err: %v", err)
	}

	if metrict.Value != nil {
		t.Fatalf("invalid value for Value: %v, want : nil", metrict.Value)
	}
}

func TestDecodeWithValueZero(t *testing.T) {
	testjson := `{"timestamp": 1790085318, "key": "input_traffic", "value": 0}`

	wantVal := 0.0

	metrict := Event{}
	if err := json.Unmarshal([]byte(testjson), &metrict); err != nil {
		t.Fatalf("error decoding json; err: %v", err)
	}

	if metrict.Value == nil {
		t.Fatalf("invalid value nil for Value, want : %v", wantVal)
	}

	if *metrict.Value != wantVal {
		t.Fatalf("invalid value for Value: %v, want : %v", metrict.Value, wantVal)
	}
}

func TestHTTPRouteMetric(t *testing.T) {
	type testCase struct {
		name       string
		body       string
		method     string
		wantStatus int
	}

	tests := []testCase{
		{
			name:       "json without Value",
			body:       `{"timestamp": 1790085318, "key": "input_traffic"}`,
			method:     "POST",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "json with value",
			body:       `{"timestamp": 1790085318, "key": "input_traffic", "value": 0}`,
			method:     "POST",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid method GET",
			method:     "GET",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "json without timestamp",
			body:       `{"key": "input_traffic", "value": 0}`,
			method:     "POST",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "json with timestamp 0",
			body:       `{"timestamp": 0, "key": "input_traffic", "value": 0}`,
			method:     "POST",
			wantStatus: http.StatusOK,
		},
		{
			name:       "json without key",
			body:       `{"timestamp": 1790085318, "value": 0}`,
			method:     "POST",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "json with key empty string",
			body:       `{"timestamp": 1790085318, "key": "", "value": 0}`,
			method:     "POST",
			wantStatus: http.StatusBadRequest,
		},
	}

	mux := NewMux()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(
				tc.method,
				"/metrics",
				strings.NewReader(tc.body),
			)
			req.Header.Set("Content-Type", "application/json")

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("received status: %d, want: %d", rec.Code, tc.wantStatus)
			}
		})
	}

}
