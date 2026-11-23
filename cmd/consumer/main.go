package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rmbarboza/go-telemetry-pipeline/internal/consumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// consumerGroup names the group whose committed offsets Kafka keeps.
	// Renaming it makes the consumer start again from the earliest offset.
	consumerGroup = "telemetry-store"
	topic         = "events"
	// commitTimeout bounds each offset commit, including the one made
	// after a shutdown signal.
	commitTimeout = 5 * time.Second
)

func run() error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(consumerGroup),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.CloseAllowingRebalance()

	ctxConsume, consumerCancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer consumerCancel()

	for {
		fetches := client.PollFetches(ctxConsume)
		if err := fetches.Err0(); errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
			slog.Info("shutdown requested")
			break
		}

		fetches.EachError(func(t string, p int32, err error) {
			slog.Error("fetch failed", "topic", t, "partition", p, "error", err)
		})

		recs, errProcess := consumer.ProcessBatch(fetches.Records())

		var errCommit error
		if len(recs) > 0 {
			ctxCommit, commitCancel := context.WithTimeout(context.Background(), commitTimeout)
			errCommit = client.CommitRecords(ctxCommit, recs...)
			commitCancel()
		}

		client.AllowRebalance()

		if errCommit != nil {
			return fmt.Errorf("commit: %w", errCommit)
		}

		if errProcess != nil {
			return fmt.Errorf("process batch: %w", errProcess)
		}
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("consumer stopped", "error", err)
		os.Exit(1)
	}
}
