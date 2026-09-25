package gotelemetrypipeline

import (
	"encoding/json"
	"io"
	"net/http"
)

func metricsHandler(w http.ResponseWriter, r *http.Request) {
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

	if metric.Timestamp == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if metric.Key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if metric.Value == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func newMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /metrics", metricsHandler)

	return mux
}
