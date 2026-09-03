package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/atlazora/atlazora-core/internal/platform/database"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestRecordConsumptionRecordsFirstDelivery(t *testing.T) {
	t.Parallel()

	db := &recordingDBTX{
		execTag: pgconn.NewCommandTag("INSERT 0 1"),
	}

	recorded, err := RecordConsumption(
		context.Background(),
		db,
		"sourcing.projector",
		"atlazora://core/sourcing",
		"018f47a2-7b3c-7abc-8def-0123456789ab",
	)
	if err != nil {
		t.Fatalf("RecordConsumption returned error: %v", err)
	}

	if !recorded {
		t.Fatal("RecordConsumption recorded = false, want true")
	}

	if db.execCalls != 1 {
		t.Fatalf("exec calls = %d, want 1", db.execCalls)
	}

	requiredSQL := []string{
		"INSERT INTO atlazora_event_consumption",
		"consumer_name",
		"event_source",
		"event_id",
		"ON CONFLICT (consumer_name, event_source, event_id)",
		"DO NOTHING",
	}

	for _, fragment := range requiredSQL {
		if !strings.Contains(db.sql, fragment) {
			t.Fatalf("consumption SQL missing %q", fragment)
		}
	}

	if len(db.args) != 3 {
		t.Fatalf("exec args = %d, want 3", len(db.args))
	}

	if db.args[0] != "sourcing.projector" {
		t.Fatalf("consumer name = %#v", db.args[0])
	}

	if db.args[1] != "atlazora://core/sourcing" {
		t.Fatalf("event source = %#v", db.args[1])
	}

	if db.args[2] != "018f47a2-7b3c-7abc-8def-0123456789ab" {
		t.Fatalf("event id = %#v", db.args[2])
	}
}

func TestRecordConsumptionReturnsFalseForDuplicateDelivery(t *testing.T) {
	t.Parallel()

	db := &recordingDBTX{
		execTag: pgconn.NewCommandTag("INSERT 0 0"),
	}

	recorded, err := RecordConsumption(
		context.Background(),
		db,
		"sourcing.projector",
		"atlazora://core/sourcing",
		"018f47a2-7b3c-7abc-8def-0123456789ab",
	)
	if err != nil {
		t.Fatalf("RecordConsumption returned error: %v", err)
	}

	if recorded {
		t.Fatal("RecordConsumption recorded = true, want false for duplicate")
	}
}

func TestRecordConsumptionPreservesLogicalConsumerBoundary(t *testing.T) {
	t.Parallel()

	firstConsumer := &recordingDBTX{
		execTag: pgconn.NewCommandTag("INSERT 0 1"),
	}

	secondConsumer := &recordingDBTX{
		execTag: pgconn.NewCommandTag("INSERT 0 1"),
	}

	eventSource := "atlazora://core/sourcing"
	eventID := "018f47a2-7b3c-7abc-8def-0123456789ab"

	recorded, err := RecordConsumption(
		context.Background(),
		firstConsumer,
		"sourcing.projector",
		eventSource,
		eventID,
	)
	if err != nil {
		t.Fatalf("first consumer: %v", err)
	}

	if !recorded {
		t.Fatal("first consumer should own first processing")
	}

	recorded, err = RecordConsumption(
		context.Background(),
		secondConsumer,
		"analytics.projector",
		eventSource,
		eventID,
	)
	if err != nil {
		t.Fatalf("second consumer: %v", err)
	}

	if !recorded {
		t.Fatal("different logical consumer must be independently recordable")
	}

	if firstConsumer.args[0] == secondConsumer.args[0] {
		t.Fatal("test did not exercise distinct logical consumers")
	}

	if firstConsumer.args[1] != secondConsumer.args[1] {
		t.Fatal("event source changed unexpectedly")
	}

	if firstConsumer.args[2] != secondConsumer.args[2] {
		t.Fatal("event id changed unexpectedly")
	}
}

func TestRecordConsumptionPreservesEventSourceBoundary(t *testing.T) {
	t.Parallel()

	firstSource := &recordingDBTX{
		execTag: pgconn.NewCommandTag("INSERT 0 1"),
	}

	secondSource := &recordingDBTX{
		execTag: pgconn.NewCommandTag("INSERT 0 1"),
	}

	consumerName := "sourcing.projector"
	eventID := "018f47a2-7b3c-7abc-8def-0123456789ab"

	recorded, err := RecordConsumption(
		context.Background(),
		firstSource,
		consumerName,
		"atlazora://core/sourcing-a",
		eventID,
	)
	if err != nil {
		t.Fatalf("first source: %v", err)
	}

	if !recorded {
		t.Fatal("first source should be recordable")
	}

	recorded, err = RecordConsumption(
		context.Background(),
		secondSource,
		consumerName,
		"atlazora://core/sourcing-b",
		eventID,
	)
	if err != nil {
		t.Fatalf("second source: %v", err)
	}

	if !recorded {
		t.Fatal("same id under different source must be independently recordable")
	}

	if firstSource.args[1] == secondSource.args[1] {
		t.Fatal("test did not exercise distinct event sources")
	}

	if firstSource.args[2] != secondSource.args[2] {
		t.Fatal("event id changed unexpectedly")
	}
}

func TestRecordConsumptionRejectsMissingIdentityInputs(t *testing.T) {
	t.Parallel()

	validDB := &recordingDBTX{}
	var typedNilDB *recordingDBTX

	tests := []struct {
		name         string
		db           database.DBTX
		consumerName string
		eventSource  string
		eventID      string
		want         error
	}{
		{
			name:         "nil database",
			db:           nil,
			consumerName: "sourcing.projector",
			eventSource:  "atlazora://core/sourcing",
			eventID:      "018f47a2-7b3c-7abc-8def-0123456789ab",
			want:         ErrDatabaseRequired,
		},
		{
			name:         "typed nil database",
			db:           typedNilDB,
			consumerName: "sourcing.projector",
			eventSource:  "atlazora://core/sourcing",
			eventID:      "018f47a2-7b3c-7abc-8def-0123456789ab",
			want:         ErrDatabaseRequired,
		},
		{
			name:         "blank consumer name",
			db:           validDB,
			consumerName: " ",
			eventSource:  "atlazora://core/sourcing",
			eventID:      "018f47a2-7b3c-7abc-8def-0123456789ab",
			want:         ErrConsumerNameRequired,
		},
		{
			name:         "blank event source",
			db:           validDB,
			consumerName: "sourcing.projector",
			eventSource:  " ",
			eventID:      "018f47a2-7b3c-7abc-8def-0123456789ab",
			want:         ErrEventSourceRequired,
		},
		{
			name:         "blank event id",
			db:           validDB,
			consumerName: "sourcing.projector",
			eventSource:  "atlazora://core/sourcing",
			eventID:      " ",
			want:         ErrEventIDRequired,
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			recorded, err := RecordConsumption(
				context.Background(),
				test.db,
				test.consumerName,
				test.eventSource,
				test.eventID,
			)

			if recorded {
				t.Fatal("RecordConsumption recorded = true, want false")
			}

			if !errors.Is(err, test.want) {
				t.Fatalf(
					"RecordConsumption error = %v, want %v",
					err,
					test.want,
				)
			}
		})
	}
}

func TestRecordConsumptionSurfacesDatabaseFailure(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("database unavailable")

	db := &recordingDBTX{
		execErr: sentinel,
	}

	recorded, err := RecordConsumption(
		context.Background(),
		db,
		"sourcing.projector",
		"atlazora://core/sourcing",
		"018f47a2-7b3c-7abc-8def-0123456789ab",
	)

	if recorded {
		t.Fatal("RecordConsumption recorded = true, want false")
	}

	if !errors.Is(err, sentinel) {
		t.Fatalf("RecordConsumption error = %v, want wrapped sentinel", err)
	}
}
