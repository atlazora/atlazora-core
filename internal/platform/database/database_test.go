package database

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestOpenRejectsInvalidConfigurationWithoutLeakingSecret(t *testing.T) {
	t.Parallel()

	const secret = "WU03-SENTINEL-PASSWORD-DO-NOT-LEAK"
	databaseURL := "postgres://user:" + secret + "@[invalid"

	pool, err := Open(context.Background(), databaseURL)
	if pool != nil {
		pool.Close()
		t.Fatal("expected no pool for invalid configuration")
	}

	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("expected ErrInvalidConfiguration, got %v", err)
	}

	if strings.Contains(err.Error(), secret) {
		t.Fatal("database error leaked the configured secret")
	}

	if strings.Contains(err.Error(), databaseURL) {
		t.Fatal("database error leaked the connection URL")
	}
}

func TestOpenConnectionFailureDoesNotLeakSecret(t *testing.T) {
	t.Parallel()

	const secret = "WU03-SENTINEL-CONNECTION-PASSWORD"

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	databaseURL := "postgres://user:" + secret + "@127.0.0.1:1/atlazora?sslmode=disable"

	pool, err := Open(ctx, databaseURL)
	if pool != nil {
		pool.Close()
		t.Fatal("expected no pool when PostgreSQL connection fails")
	}

	if !errors.Is(err, ErrConnectionFailed) {
		t.Fatalf("expected ErrConnectionFailed, got %v", err)
	}

	if strings.Contains(err.Error(), secret) {
		t.Fatal("connection error leaked the configured secret")
	}

	if strings.Contains(err.Error(), databaseURL) {
		t.Fatal("connection error leaked the connection URL")
	}
}
