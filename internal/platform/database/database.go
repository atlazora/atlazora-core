package database

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultConnectTimeout = 5 * time.Second

var (
	ErrInvalidConfiguration = errors.New("invalid PostgreSQL configuration")
	ErrConnectionFailed     = errors.New("establish PostgreSQL connection")
)

// DBTX is the common PostgreSQL execution surface available both on a pool
// and inside a transaction. Persistence code that must participate in an
// existing transaction should depend on this interface instead of opening
// an independent database operation.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

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

// WithinTransaction executes fn inside one PostgreSQL transaction.
//
// The callback receives the transaction itself as the DBTX boundary so
// authoritative state changes and durable outbox intent can be written by
// separate persistence components while sharing the same atomic commit.
//
// A callback error causes rollback. Commit errors are returned to the caller.
// Rollback is deferred as a safety net after the callback returns.
func WithinTransaction(
	ctx context.Context,
	pool *pgxpool.Pool,
	fn func(context.Context, DBTX) error,
) error {
	if pool == nil {
		return errors.New("database pool is required")
	}

	if fn == nil {
		return errors.New("transaction callback is required")
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := fn(ctx, tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
