package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadUsesDefaults(t *testing.T) {
	t.Setenv("ATLAZORA_ENV", "")
	t.Setenv("ATLAZORA_HTTP_ADDR", "")
	t.Setenv("ATLAZORA_DATABASE_URL", "postgres://user:password@localhost:5432/atlazora?sslmode=disable")
	t.Setenv("ATLAZORA_SHUTDOWN_TIMEOUT", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Environment != "development" {
		t.Fatalf("Environment = %q, want development", cfg.Environment)
	}

	if cfg.HTTPAddress != ":8080" {
		t.Fatalf("HTTPAddress = %q, want :8080", cfg.HTTPAddress)
	}

	if cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want 10s", cfg.ShutdownTimeout)
	}
}

func TestLoadReadsExplicitValues(t *testing.T) {
	t.Setenv("ATLAZORA_ENV", "staging")
	t.Setenv("ATLAZORA_HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("ATLAZORA_DATABASE_URL", "postgres://user:password@localhost:5432/atlazora")
	t.Setenv("ATLAZORA_SHUTDOWN_TIMEOUT", "25s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Environment != "staging" {
		t.Fatalf("Environment = %q, want staging", cfg.Environment)
	}

	if cfg.HTTPAddress != "127.0.0.1:9090" {
		t.Fatalf("HTTPAddress = %q, want 127.0.0.1:9090", cfg.HTTPAddress)
	}

	if cfg.ShutdownTimeout != 25*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want 25s", cfg.ShutdownTimeout)
	}
}

func TestLoadRejectsMissingDatabaseURL(t *testing.T) {
	t.Setenv("ATLAZORA_ENV", "test")
	t.Setenv("ATLAZORA_DATABASE_URL", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want missing database URL error")
	}

	if !strings.Contains(err.Error(), "ATLAZORA_DATABASE_URL is required") {
		t.Fatalf("Load() error = %q, want database URL validation error", err)
	}
}

func TestLoadRejectsInvalidEnvironment(t *testing.T) {
	t.Setenv("ATLAZORA_ENV", "invalid")
	t.Setenv("ATLAZORA_DATABASE_URL", "postgres://example")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want invalid environment error")
	}

	if !strings.Contains(err.Error(), "unsupported ATLAZORA_ENV") {
		t.Fatalf("Load() error = %q, want environment validation error", err)
	}
}

func TestLoadRejectsInvalidShutdownTimeout(t *testing.T) {
	t.Setenv("ATLAZORA_ENV", "test")
	t.Setenv("ATLAZORA_DATABASE_URL", "postgres://example")
	t.Setenv("ATLAZORA_SHUTDOWN_TIMEOUT", "not-a-duration")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want duration parsing error")
	}

	if !strings.Contains(err.Error(), "parse ATLAZORA_SHUTDOWN_TIMEOUT") {
		t.Fatalf("Load() error = %q, want shutdown timeout parsing error", err)
	}
}

func TestValidateRejectsNonPositiveShutdownTimeout(t *testing.T) {
	cfg := Config{
		Environment:     "test",
		HTTPAddress:     ":8080",
		DatabaseURL:     "postgres://example",
		ShutdownTimeout: 0,
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want shutdown timeout validation error")
	}
}
