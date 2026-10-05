package gotelemetrypipeline

import (
	"errors"
)

type Event struct {
	Timestamp *uint64  `json:"timestamp"`
	Key       string   `json:"key"`
	Value     *float64 `json:"value"`
}

func validateEvent(e Event) error {
	if e.Timestamp == nil {
		return errors.New("missing timestamp")
	}

	if e.Key == "" {
		return errors.New("missing key")
	}

	if e.Value == nil {
		return errors.New("missing value")
	}

	return nil
}
