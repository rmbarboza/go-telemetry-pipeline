package gotelemetrypipeline

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

const validID = "01890123-4567-7890-a123-456789abcdef"

func TestDecodeWithSuccess(t *testing.T) {
	testjson := `{"timestamp": 1790085318, "key": "input_traffic", "value": 100.5}`

	wantTs := uint64(1790085318)
	wantKey := "input_traffic"
	wantVal := 100.5

	metric := Event{}
	if err := json.Unmarshal([]byte(testjson), &metric); err != nil {
		t.Fatalf("error decoding json; err: %v", err)
	}

	if metric.Timestamp == nil {
		t.Fatal("missing timestamp in received json")
	}

	if *metric.Timestamp != wantTs {
		t.Fatalf("invalid value for timestamp: %v, want : %v", metric.Timestamp, wantTs)
	}

	if metric.Key != wantKey {
		t.Fatalf("invalid value for Key: %v, want : %v", metric.Key, wantKey)
	}

	if metric.Value == nil {
		t.Fatalf("invalid value nil for Value, want : %v", wantVal)
	}

	if *metric.Value != wantVal {
		t.Fatalf("invalid value for Value: %v, want : %v", metric.Value, wantVal)
	}
}

func TestDecodeWithFail(t *testing.T) {
	testjson := `{"timestamp": 1790085318, "key": "input_traffic", "value": "100.5"}`

	metric := Event{}
	if err := json.Unmarshal([]byte(testjson), &metric); err == nil {
		t.Fatal("expected an error for value encoded as string")
	}
}

func TestDecodeWithoutValue(t *testing.T) {
	testjson := `{"timestamp": 1790085318, "key": "input_traffic"}`

	metric := Event{}
	if err := json.Unmarshal([]byte(testjson), &metric); err != nil {
		t.Fatalf("error decoding json; err: %v", err)
	}

	if metric.Value != nil {
		t.Fatalf("invalid value for Value: %v, want : nil", metric.Value)
	}
}

func TestDecodeWithValueZero(t *testing.T) {
	testjson := `{"timestamp": 1790085318, "key": "input_traffic", "value": 0}`

	wantVal := 0.0

	metric := Event{}
	if err := json.Unmarshal([]byte(testjson), &metric); err != nil {
		t.Fatalf("error decoding json; err: %v", err)
	}

	if metric.Value == nil {
		t.Fatalf("invalid value nil for Value, want : %v", wantVal)
	}

	if *metric.Value != wantVal {
		t.Fatalf("invalid value for Value: %v, want : %v", metric.Value, wantVal)
	}
}

func TestHTTPRouteMetric(t *testing.T) {
	type testCase struct {
		name       string
		body       string
		method     string
		wantStatus int
	}

	now := time.Now()

	tests := []testCase{
		{
			name:       "json without Value",
			body:       `{"id": "` + validID + `","timestamp": ` + strconv.FormatInt(now.Unix(), 10) + `, "key": "input_traffic"}`,
			method:     "POST",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "json with value",
			body:       `{"id": "` + validID + `","timestamp": ` + strconv.FormatInt(now.Unix(), 10) + `, "key": "input_traffic", "value": 0}`,
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
			body:       `{"id": "` + validID + `","key": "input_traffic", "value": 0}`,
			method:     "POST",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "json with timestamp 0",
			body:       `{"id": "` + validID + `","timestamp": 0, "key": "input_traffic", "value": 0}`,
			method:     "POST",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "json without key",
			body:       `{"id": "` + validID + `","timestamp": ` + strconv.FormatInt(now.Unix(), 10) + `, "value": 0}`,
			method:     "POST",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "json with key empty string",
			body:       `{"id": "` + validID + `","timestamp": ` + strconv.FormatInt(now.Unix(), 10) + `, "key": "", "value": 0}`,
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

func TestValidateEvent(t *testing.T) {
	type testCase struct {
		name    string
		event   Event
		wantErr bool
	}

	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	validTs := uint64(now.Unix())

	tests := []testCase{
		{
			name:    "valid id",
			event:   Event{ID: validID, Timestamp: new(validTs), Key: "input_traffic", Value: new(100.0)},
			wantErr: false,
		},
		{
			name:    "255 x ç in key",
			event:   Event{ID: validID, Timestamp: new(validTs), Key: strings.Repeat("ç", 255), Value: new(100.0)},
			wantErr: false,
		},
		{
			name:    "missing id",
			event:   Event{Timestamp: new(validTs), Key: "input_traffic", Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    "id = abc",
			event:   Event{ID: "abc", Timestamp: new(validTs), Key: "input_traffic", Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    "id v4",
			event:   Event{ID: "380b7a52-7f5b-44ee-a71a-e33727055168", Timestamp: new(validTs), Key: "input_traffic", Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    "id with brackets",
			event:   Event{ID: "{01890123-4567-7890-a123-456789abcdef}", Timestamp: new(validTs), Key: "input_traffic", Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    "id without hifens",
			event:   Event{ID: "0189012345677890a123456789abcdef", Timestamp: new(validTs), Key: "input_traffic", Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    "256 x a in key",
			event:   Event{ID: validID, Timestamp: new(validTs), Key: strings.Repeat("a", 256), Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    `a\x00b in key`,
			event:   Event{ID: validID, Timestamp: new(validTs), Key: "a\x00b", Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    "valid time now",
			event:   Event{ID: validID, Timestamp: new(validTs), Key: "input_traffic", Value: new(100.0)},
			wantErr: false,
		},
		{
			name:    "valid time now + 5min",
			event:   Event{ID: validID, Timestamp: new(uint64(now.Add(5 * time.Minute).Unix())), Key: "input_traffic", Value: new(100.0)},
			wantErr: false,
		},
		{
			name:    "invalid time now + 5min + 1s",
			event:   Event{ID: validID, Timestamp: new(uint64(now.Add(5*time.Minute + time.Second).Unix())), Key: "input_traffic", Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    "valid time now - 7d",
			event:   Event{ID: validID, Timestamp: new(uint64(now.Add(-7 * 24 * time.Hour).Unix())), Key: "input_traffic", Value: new(100.0)},
			wantErr: false,
		},
		{
			name:    "invalid time now - 7d - 1s",
			event:   Event{ID: validID, Timestamp: new(uint64(now.Add(-7*24*time.Hour - time.Second).Unix())), Key: "input_traffic", Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    "invalid time 0",
			event:   Event{ID: validID, Timestamp: new(uint64(0)), Key: "input_traffic", Value: new(100.0)},
			wantErr: true,
		},
		{
			name:    "invalid time max uint64",
			event:   Event{ID: validID, Timestamp: new(uint64(math.MaxUint64)), Key: "input_traffic", Value: new(100.0)},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			err := validateEvent(tc.event, now)

			if (err == nil) == tc.wantErr {
				t.Errorf("validateEvent() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}

}
