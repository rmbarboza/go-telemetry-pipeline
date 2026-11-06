package gotelemetrypipeline

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

type KafkaPublisher struct {
	client *kgo.Client
}

// NewKafkaPublisher returns a Publisher backed by c.
// It panics if c is nil.
func NewKafkaPublisher(c *kgo.Client) *KafkaPublisher {
	if c == nil {
		panic("gotelemetrypipeline: NewKafkaPublisher called with nil client")
	}
	return &KafkaPublisher{client: c}
}

func (k *KafkaPublisher) Publish(ctx context.Context, event Event) error {
	// Convert the Event to a Kafka record

	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", event.ID, err)
	}

	record := &kgo.Record{
		Value: msg,
	}

	// Produce the record to Kafka
	err = k.client.ProduceSync(ctx, record).FirstErr()
	if err != nil {
		return fmt.Errorf("publish event %s: %w", event.ID, err)
	}

	return nil
}
