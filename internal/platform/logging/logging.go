package logging

import (
	"io"
	"log/slog"
	"strings"
)

// New creates the foundational structured JSON logger for Atlazora Core.
func New(output io.Writer, environment string) *slog.Logger {
	level := slog.LevelInfo

	if strings.EqualFold(environment, "development") {
		level = slog.LevelDebug
	}

	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level: level,
	})

	return slog.New(handler).With(
		slog.String("service", "atlazora-core"),
		slog.String("environment", environment),
	)
}
