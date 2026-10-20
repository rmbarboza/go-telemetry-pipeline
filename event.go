package gotelemetrypipeline

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

type Event struct {
	ID        string   `json:"id"`
	Timestamp *uint64  `json:"timestamp"`
	Key       string   `json:"key"`
	Value     *float64 `json:"value"`
}

const (
	maxFutureSkew = 5 * time.Minute
	maxEventAge   = 7 * 24 * time.Hour
)

func validateEvent(e Event, now time.Time) error {
	if e.ID == "" {
		return errors.New("missing id")
	}

	if len(e.ID) != 36 {
		return errors.New("id is not in canonical version")
	}

	eventUUID, err := uuid.Parse(e.ID)
	if err != nil {
		return fmt.Errorf("parse id: %w", err)
	}

	if eventUUID.Version() != 7 {
		return fmt.Errorf("uuid must be in version 7: %s, version: %d", eventUUID, eventUUID.Version())
	}

	if e.Timestamp == nil {
		return errors.New("missing timestamp")
	}

	maxTs := uint64(now.Add(maxFutureSkew).Unix())
	minTs := uint64(now.Add(-maxEventAge).Unix())

	if *e.Timestamp > maxTs {
		return fmt.Errorf("timestamp %d is after max allowed %d", *e.Timestamp, maxTs)
	}

	if *e.Timestamp < minTs {
		return fmt.Errorf("timestamp %d is before min allowed %d", *e.Timestamp, minTs)
	}

	if e.Key == "" {
		return errors.New("missing key")
	}

	if strings.ContainsRune(e.Key, 0) {
		return errors.New("invalid null byte in key")
	}

	if utf8.RuneCountInString(e.Key) > 255 {
		return errors.New("number of characters in key is bigger than 255")
	}

	if e.Value == nil {
		return errors.New("missing value")
	}

	return nil
}
