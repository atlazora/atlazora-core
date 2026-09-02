package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	defaultEnvironment     = "development"
	defaultHTTPAddress     = ":8080"
	defaultShutdownTimeout = 10 * time.Second
)

// Config contains foundational runtime configuration for Core processes.
type Config struct {
	Environment     string
	HTTPAddress     string
	DatabaseURL     string
	ShutdownTimeout time.Duration
}

// Load reads configuration from environment variables and validates it.
func Load() (Config, error) {
	cfg := Config{
		Environment:     valueOrDefault("ATLAZORA_ENV", defaultEnvironment),
		HTTPAddress:     valueOrDefault("ATLAZORA_HTTP_ADDR", defaultHTTPAddress),
		DatabaseURL:     strings.TrimSpace(os.Getenv("ATLAZORA_DATABASE_URL")),
		ShutdownTimeout: defaultShutdownTimeout,
	}

	shutdownRaw := strings.TrimSpace(os.Getenv("ATLAZORA_SHUTDOWN_TIMEOUT"))
	if shutdownRaw != "" {
		shutdownTimeout, err := time.ParseDuration(shutdownRaw)
		if err != nil {
			return Config{}, fmt.Errorf("parse ATLAZORA_SHUTDOWN_TIMEOUT: %w", err)
		}
		cfg.ShutdownTimeout = shutdownTimeout
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Validate verifies foundational configuration invariants.
func (c Config) Validate() error {
	if !validEnvironment(c.Environment) {
		return fmt.Errorf("unsupported ATLAZORA_ENV %q", c.Environment)
	}

	if strings.TrimSpace(c.HTTPAddress) == "" {
		return errors.New("ATLAZORA_HTTP_ADDR must not be empty")
	}

	if strings.TrimSpace(c.DatabaseURL) == "" {
		return errors.New("ATLAZORA_DATABASE_URL is required")
	}

	if c.ShutdownTimeout <= 0 {
		return errors.New("ATLAZORA_SHUTDOWN_TIMEOUT must be greater than zero")
	}

	return nil
}

func valueOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func validEnvironment(environment string) bool {
	switch environment {
	case "development", "test", "staging", "production":
		return true
	default:
		return false
	}
}
