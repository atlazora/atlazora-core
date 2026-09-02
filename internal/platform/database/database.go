package database

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultConnectTimeout = 5 * time.Second

var (
	ErrInvalidConfiguration = errors.New("invalid PostgreSQL configuration")
	ErrConnectionFailed     = errors.New("establish PostgreSQL connection")
)

// Open establishes the foundational PostgreSQL connection pool.
// Returned errors intentionally do not expose connection configuration.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, ErrInvalidConfiguration
	}

	connectCtx, cancel := context.WithTimeout(ctx, defaultConnectTimeout)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(connectCtx, config)
	if err != nil {
		return nil, ErrConnectionFailed
	}

	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		return nil, ErrConnectionFailed
	}

	return pool, nil
}
