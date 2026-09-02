package logging

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNewWritesStructuredJSON(t *testing.T) {
	var buffer bytes.Buffer

	logger := New(&buffer, "test")
	logger.Info("core started", "component", "test")

	var entry map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &entry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	if entry["service"] != "atlazora-core" {
		t.Fatalf("service = %v, want atlazora-core", entry["service"])
	}

	if entry["environment"] != "test" {
		t.Fatalf("environment = %v, want test", entry["environment"])
	}

	if entry["msg"] != "core started" {
		t.Fatalf("msg = %v, want core started", entry["msg"])
	}

	if entry["component"] != "test" {
		t.Fatalf("component = %v, want test", entry["component"])
	}
}

func TestDevelopmentEnablesDebugLogging(t *testing.T) {
	var buffer bytes.Buffer

	logger := New(&buffer, "development")
	logger.Debug("debug enabled")

	if buffer.Len() == 0 {
		t.Fatal("development logger did not emit debug output")
	}
}

func TestProductionSuppressesDebugLogging(t *testing.T) {
	var buffer bytes.Buffer

	logger := New(&buffer, "production")
	logger.Debug("debug should not be emitted")

	if buffer.Len() != 0 {
		t.Fatalf("production logger emitted debug output: %s", buffer.String())
	}
}
