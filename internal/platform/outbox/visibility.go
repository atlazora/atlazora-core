package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/atlazora/atlazora-core/internal/platform/database"
)

// VisibilitySnapshot exposes the minimum persisted operational signals required
// by the WU05 event/outbox foundation.
//
// This is intentionally a read-only database snapshot, not a metrics,
// telemetry, alerting, dashboard, or health-check framework.
type VisibilitySnapshot struct {
	ReadyBacklogCount      int64
	ProcessingCount        int64
	FailedPendingCount     int64
	RetriedEventCount      int64
	ConsumptionMarkerCount int64
	OldestReadyCreatedAt   *time.Time
}

// OldestReadyLag returns the age of the oldest currently-ready pending outbox
// record at the supplied observation time.
//
// The caller supplies now so this foundation does not choose a clock,
// collection cadence, alert threshold, or SLO.
func (snapshot VisibilitySnapshot) OldestReadyLag(now time.Time) time.Duration {
	if snapshot.OldestReadyCreatedAt == nil {
		return 0
	}

	lag := now.Sub(*snapshot.OldestReadyCreatedAt)
	if lag < 0 {
		return 0
	}

	return lag
}

// ReadVisibilitySnapshot reads foundational outbox and idempotency operational
// state without mutating either table.
func ReadVisibilitySnapshot(
	ctx context.Context,
	db database.DBTX,
) (VisibilitySnapshot, error) {
	if !dbRequired(db) {
		return VisibilitySnapshot{}, ErrDatabaseRequired
	}

	const statement = `
SELECT
COUNT(*) FILTER (
WHERE state = 'pending'
  AND available_at <= CURRENT_TIMESTAMP
)::bigint AS ready_backlog_count,
COUNT(*) FILTER (
WHERE state = 'processing'
)::bigint AS processing_count,
COUNT(*) FILTER (
WHERE state = 'pending'
  AND last_error IS NOT NULL
)::bigint AS failed_pending_count,
COUNT(*) FILTER (
WHERE attempt_count > 1
)::bigint AS retried_event_count,
MIN(created_at) FILTER (
WHERE state = 'pending'
  AND available_at <= CURRENT_TIMESTAMP
) AS oldest_ready_created_at,
(
SELECT COUNT(*)::bigint
FROM atlazora_event_consumption
) AS consumption_marker_count
FROM atlazora_outbox
`

	var snapshot VisibilitySnapshot

	if err := db.QueryRow(
		ctx,
		statement,
	).Scan(
		&snapshot.ReadyBacklogCount,
		&snapshot.ProcessingCount,
		&snapshot.FailedPendingCount,
		&snapshot.RetriedEventCount,
		&snapshot.OldestReadyCreatedAt,
		&snapshot.ConsumptionMarkerCount,
	); err != nil {
		return VisibilitySnapshot{}, fmt.Errorf("read outbox visibility snapshot: %w", err)
	}

	return snapshot, nil
}
