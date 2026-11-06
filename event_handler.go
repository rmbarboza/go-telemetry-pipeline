package gotelemetrypipeline

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const (
	// Timeout to publish an event to the publisher.
	publishTimeout = 5 * time.Second
	// Retry-After header value in seconds when the publisher fails to publish an event.
	retryAfterSeconds = "5"
)

func newMetricsHandler(p Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		metric := Event{}
		if err := json.Unmarshal(body, &metric); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := validateEvent(metric, time.Now()); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), publishTimeout)
		defer cancel()

		if err := p.Publish(ctx, metric); err != nil {
			slog.Error("publish failed", "error", err)
			w.Header().Set("Retry-After", retryAfterSeconds)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}
}

func NewMux(p Publisher) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /metrics", newMetricsHandler(p))

	return mux
}
