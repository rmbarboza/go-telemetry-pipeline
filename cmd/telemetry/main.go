package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

import gotelemetrypipeline "github.com/rmbarboza/go-telemetry-pipeline"

func main() {

	mux := gotelemetrypipeline.NewMux()

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server := http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error(
				"Unexpected server shutdown",
				"error", err,
			)
			os.Exit(1)
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
				"Shutdown finished",
				"duration_ms", time.Since(begin).Milliseconds(),
			)
		} else {
			slog.Error(
				"Shutdown failed",
				"duration_ms", time.Since(begin).Milliseconds(),
				"error", err,
			)
			os.Exit(1)
		}
	}
}
