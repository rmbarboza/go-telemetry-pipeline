package httpapi

import (
	"context"

	"github.com/rmbarboza/go-telemetry-pipeline/internal/event"
)

type Publisher interface {
	Publish(ctx context.Context, e event.Event) error
}
