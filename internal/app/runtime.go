package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/atlazora/atlazora-core/internal/platform/config"
	"github.com/atlazora/atlazora-core/internal/platform/database"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Runtime contains foundational resources shared by Core processes.
type Runtime struct {
	Config   config.Config
	Logger   *slog.Logger
	Database *pgxpool.Pool
}

// NewRuntime establishes foundational runtime resources.
func NewRuntime(ctx context.Context, cfg config.Config, logger *slog.Logger) (*Runtime, error) {
	pool, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("initialize database: %w", err)
	}

	return &Runtime{
		Config:   cfg,
		Logger:   logger,
		Database: pool,
	}, nil
}

// Close releases foundational runtime resources.
func (r *Runtime) Close() {
	if r == nil {
		return
	}

	if r.Database != nil {
		r.Database.Close()
	}
}
