package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	gotelemetrypipeline "github.com/rmbarboza/go-telemetry-pipeline"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	readHeaderTimeout = 3 * time.Second
	readTimeout       = 5 * time.Second
	writeMargin       = 1 * time.Second
	// ADR 0003: publish deadline < WriteTimeout < client timeout.
	writeTimeout = readTimeout + gotelemetrypipeline.PublishTimeout + writeMargin
	idleTimeout  = 60 * time.Second
)

func run() error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.DefaultProduceTopic("events"),
	)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close()
	pub := gotelemetrypipeline.NewKafkaPublisher(client)
	mux := gotelemetrypipeline.NewMux(pub)

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server := http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("unexpected server shutdown: %w", err)
		}
	case <-signalCtx.Done():
		slog.Info("shutdown requested")
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		begin := time.Now()
		err := server.Shutdown(shutdownCtx)

		if err == nil {
			slog.Info(
				"shutdown finished",
				"duration_ms", time.Since(begin).Milliseconds(),
			)
		} else {
			return fmt.Errorf("shutdown after %d ms: %w", time.Since(begin).Milliseconds(), err)
		}
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("telemetry stopped", "error", err)
		os.Exit(1)
	}
}
