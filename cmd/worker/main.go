package main

import (
	"context"
	"log/slog"
	"os"

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

	logger := logging.New(os.Stdout, cfg.Environment).With("process", "worker")

	ctx, stop := lifecycle.SignalContext(context.Background())
	defer stop()

	runtime, err := app.NewRuntime(ctx, cfg, logger)
	if err != nil {
		logger.Error("runtime initialization failed", "error", err)
		return 1
	}
	defer runtime.Close()

	logger.Info("worker started")

	<-ctx.Done()

	logger.Info("worker shutdown requested")
	logger.Info("worker stopped")

	return 0
}
