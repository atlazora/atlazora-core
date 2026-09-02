package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/atlazora/atlazora-core/internal/app"
	"github.com/atlazora/atlazora-core/internal/platform/config"
	"github.com/atlazora/atlazora-core/internal/platform/lifecycle"
	"github.com/atlazora/atlazora-core/internal/platform/logging"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration failed", "error", err)
		return 1
	}

	logger := logging.New(os.Stdout, cfg.Environment).With("process", "api")

	ctx, stop := lifecycle.SignalContext(context.Background())
	defer stop()

	runtime, err := app.NewRuntime(ctx, cfg, logger)
	if err != nil {
		logger.Error("runtime initialization failed", "error", err)
		return 1
	}
	defer runtime.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{\"status\":\"ok\"}"))
	})

	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)

	go func() {
		logger.Info("api started", "address", cfg.HTTPAddress)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("api shutdown requested")
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server failed", "error", err)
			return 1
		}
		return 0
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("api graceful shutdown failed", "error", err)
		return 1
	}

	logger.Info("api stopped")
	return 0
}
