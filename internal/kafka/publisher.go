package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rmbarboza/go-telemetry-pipeline/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher struct {
	client *kgo.Client
}

// NewPublisher returns a Publisher backed by c.
// It panics if c is nil.
func NewPublisher(c *kgo.Client) *Publisher {
	if c == nil {
		panic("kafka: NewPublisher called with nil client")
	}
	return &Publisher{client: c}
}

func (k *Publisher) Publish(ctx context.Context, e event.Event) error {
	// Convert the Event to a Kafka record

	msg, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", e.ID, err)
	}

	record := &kgo.Record{
		Value: msg,
	}

	// Produce the record to Kafka
	err = k.client.ProduceSync(ctx, record).FirstErr()
	if err != nil {
		return fmt.Errorf("publish event %s: %w", e.ID, err)
	}

	return nil
}
