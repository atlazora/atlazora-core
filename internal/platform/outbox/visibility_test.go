package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/atlazora/atlazora-core/internal/platform/database"

	"github.com/jackc/pgx/v5"
)

type visibilitySnapshotRow struct {
	values []any
	err    error
}

func (row visibilitySnapshotRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}

	if len(dest) != len(row.values) {
		return errors.New("destination count mismatch")
	}

	for i, value := range row.values {
		switch target := dest[i].(type) {
		case *int64:
			*target = value.(int64)
		case **time.Time:
			if value == nil {
				*target = nil
				continue
			}

			timestamp := value.(time.Time)
			*target = &timestamp
		default:
			return errors.New("unsupported visibility scan destination")
		}
	}

	return nil
}
func TestReadVisibilitySnapshotReadsFoundationSignals(t *testing.T) {
	t.Parallel()

	oldest := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	db := &recordingDBTX{
		row: pgx.Row(visibilitySnapshotRow{
			values: []any{
				int64(7),
				int64(2),
				int64(3),
				int64(4),
				oldest,
				int64(11),
			},
		}),
	}

	snapshot, err := ReadVisibilitySnapshot(
		context.Background(),
		db,
	)
	if err != nil {
		t.Fatalf("ReadVisibilitySnapshot returned error: %v", err)
	}

	if snapshot.ReadyBacklogCount != 7 {
		t.Fatalf("ready backlog = %d, want 7", snapshot.ReadyBacklogCount)
	}

	if snapshot.ProcessingCount != 2 {
		t.Fatalf("processing count = %d, want 2", snapshot.ProcessingCount)
	}

	if snapshot.FailedPendingCount != 3 {
		t.Fatalf("failed pending = %d, want 3", snapshot.FailedPendingCount)
	}

	if snapshot.RetriedEventCount != 4 {
		t.Fatalf("retried events = %d, want 4", snapshot.RetriedEventCount)
	}

	if snapshot.ConsumptionMarkerCount != 11 {
		t.Fatalf(
			"consumption markers = %d, want 11",
			snapshot.ConsumptionMarkerCount,
		)
	}

	if snapshot.OldestReadyCreatedAt == nil {
		t.Fatal("oldest ready created_at = nil")
	}

	if !snapshot.OldestReadyCreatedAt.Equal(oldest) {
		t.Fatalf(
			"oldest ready created_at = %v, want %v",
			*snapshot.OldestReadyCreatedAt,
			oldest,
		)
	}

	requiredSQL := []string{
		"state = 'pending'",
		"available_at <= CURRENT_TIMESTAMP",
		"state = 'processing'",
		"last_error IS NOT NULL",
		"attempt_count > 1",
		"MIN(created_at)",
		"atlazora_event_consumption",
	}

	for _, fragment := range requiredSQL {
		if !strings.Contains(db.queryRowSQL, fragment) {
			t.Fatalf("visibility SQL missing %q", fragment)
		}
	}
}

func TestVisibilitySnapshotOldestReadyLag(t *testing.T) {
	t.Parallel()

	oldest := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	snapshot := VisibilitySnapshot{
		OldestReadyCreatedAt: &oldest,
	}

	now := oldest.Add(37 * time.Second)

	if got := snapshot.OldestReadyLag(now); got != 37*time.Second {
		t.Fatalf("lag = %v, want %v", got, 37*time.Second)
	}
}

func TestVisibilitySnapshotOldestReadyLagWithoutBacklog(t *testing.T) {
	t.Parallel()

	var snapshot VisibilitySnapshot

	if got := snapshot.OldestReadyLag(time.Now()); got != 0 {
		t.Fatalf("lag = %v, want 0", got)
	}
}

func TestVisibilitySnapshotOldestReadyLagDoesNotGoNegative(t *testing.T) {
	t.Parallel()

	oldest := time.Date(2026, 9, 3, 12, 0, 1, 0, time.UTC)

	snapshot := VisibilitySnapshot{
		OldestReadyCreatedAt: &oldest,
	}

	now := oldest.Add(-time.Second)

	if got := snapshot.OldestReadyLag(now); got != 0 {
		t.Fatalf("lag = %v, want 0", got)
	}
}

func TestReadVisibilitySnapshotRejectsMissingDatabase(t *testing.T) {
	t.Parallel()

	var typedNilDB *recordingDBTX

	for _, db := range []database.DBTX{
		nil,
		typedNilDB,
	} {
		snapshot, err := ReadVisibilitySnapshot(
			context.Background(),
			db,
		)

		if !errors.Is(err, ErrDatabaseRequired) {
			t.Fatalf("error = %v, want %v", err, ErrDatabaseRequired)
		}

		if snapshot != (VisibilitySnapshot{}) {
			t.Fatalf("snapshot = %#v, want zero value", snapshot)
		}
	}
}
