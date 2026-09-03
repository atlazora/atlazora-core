package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type recordingPublisher struct {
	calls  int
	record Record
	err    error
}

func (publisher *recordingPublisher) Publish(
	_ context.Context,
	record Record,
) error {
	publisher.calls++
	publisher.record = record
	return publisher.err
}

func claimedRecordRow(attemptCount int) pgx.Row {
	return stubRow{
		values: []any{
			int64(41),
			"018f47a2-7b3c-7abc-8def-0123456789ab",
			"atlazora://core/test",
			"com.atlazora.test.created.v1",
			time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
			"https://contracts.atlazora.test/events/test-created/v1",
			[]byte(
				`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-0123456789ab"}`,
			),
			attemptCount,
		},
	}
}

func TestProcessOnePublishesAndMarksClaimedEvent(t *testing.T) {
	t.Parallel()

	db := &recordingDBTX{
		row:     claimedRecordRow(1),
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}

	publisher := &recordingPublisher{}

	processed, err := ProcessOne(
		context.Background(),
		db,
		publisher,
		"worker-a",
		time.Date(2026, 9, 3, 12, 5, 0, 0, time.UTC),
		time.Date(2026, 9, 3, 12, 1, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("ProcessOne returned error: %v", err)
	}

	if !processed {
		t.Fatal("ProcessOne processed = false, want true")
	}

	if publisher.calls != 1 {
		t.Fatalf("publisher calls = %d, want 1", publisher.calls)
	}

	if publisher.record.EventID != "018f47a2-7b3c-7abc-8def-0123456789ab" {
		t.Fatalf("published event id = %q", publisher.record.EventID)
	}

	if publisher.record.EventSource != "atlazora://core/test" {
		t.Fatalf("published event source = %q", publisher.record.EventSource)
	}

	if publisher.record.AttemptCount != 1 {
		t.Fatalf(
			"published attempt count = %d, want 1",
			publisher.record.AttemptCount,
		)
	}

	if db.execCalls != 1 {
		t.Fatalf("lifecycle exec calls = %d, want 1", db.execCalls)
	}

	if !strings.Contains(db.sql, "state = 'published'") {
		t.Fatal("successful publication did not execute MarkPublished")
	}

	if len(db.args) != 2 {
		t.Fatalf("MarkPublished args = %d, want 2", len(db.args))
	}

	if db.args[0] != int64(41) {
		t.Fatalf("MarkPublished outbox id = %#v, want 41", db.args[0])
	}

	if db.args[1] != "worker-a" {
		t.Fatalf("MarkPublished lease owner = %#v, want worker-a", db.args[1])
	}
}

func TestProcessOneReleasesFailedPublication(t *testing.T) {
	t.Parallel()

	publishFailure := errors.New("transport unavailable")

	db := &recordingDBTX{
		row:     claimedRecordRow(1),
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}

	publisher := &recordingPublisher{
		err: publishFailure,
	}

	failureAvailableAt := time.Date(
		2026,
		9,
		3,
		12,
		2,
		0,
		0,
		time.UTC,
	)

	processed, err := ProcessOne(
		context.Background(),
		db,
		publisher,
		"worker-a",
		time.Date(2026, 9, 3, 12, 5, 0, 0, time.UTC),
		failureAvailableAt,
	)

	if !processed {
		t.Fatal("ProcessOne processed = false, want true")
	}

	if !errors.Is(err, publishFailure) {
		t.Fatalf("ProcessOne error = %v, want wrapped publish failure", err)
	}

	if publisher.calls != 1 {
		t.Fatalf("publisher calls = %d, want 1", publisher.calls)
	}

	if db.execCalls != 1 {
		t.Fatalf("release exec calls = %d, want 1", db.execCalls)
	}

	if !strings.Contains(db.sql, "state = 'pending'") {
		t.Fatal("failed publication did not execute ReleaseFailed")
	}

	if !strings.Contains(db.sql, "available_at = $3") {
		t.Fatal("ReleaseFailed did not persist caller-supplied availability")
	}

	if len(db.args) != 4 {
		t.Fatalf("ReleaseFailed args = %d, want 4", len(db.args))
	}

	if db.args[0] != int64(41) {
		t.Fatalf("ReleaseFailed outbox id = %#v, want 41", db.args[0])
	}

	if db.args[1] != "worker-a" {
		t.Fatalf("ReleaseFailed lease owner = %#v, want worker-a", db.args[1])
	}

	if got, ok := db.args[2].(time.Time); !ok || !got.Equal(failureAvailableAt) {
		t.Fatalf(
			"ReleaseFailed available_at = %#v, want %s",
			db.args[2],
			failureAvailableAt,
		)
	}

	if db.args[3] != publishFailure.Error() {
		t.Fatalf(
			"ReleaseFailed last_error = %#v, want %q",
			db.args[3],
			publishFailure.Error(),
		)
	}
}

func TestProcessOneReturnsFalseWhenNothingClaimable(t *testing.T) {
	t.Parallel()

	db := &recordingDBTX{
		row: stubRow{err: pgx.ErrNoRows},
	}

	publisher := &recordingPublisher{}

	processed, err := ProcessOne(
		context.Background(),
		db,
		publisher,
		"worker-a",
		time.Date(2026, 9, 3, 12, 5, 0, 0, time.UTC),
		time.Date(2026, 9, 3, 12, 1, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("ProcessOne returned error: %v", err)
	}

	if processed {
		t.Fatal("ProcessOne processed = true, want false")
	}

	if publisher.calls != 0 {
		t.Fatalf("publisher calls = %d, want 0", publisher.calls)
	}

	if db.execCalls != 0 {
		t.Fatalf("lifecycle exec calls = %d, want 0", db.execCalls)
	}
}

func TestProcessOneReportsReleaseFailure(t *testing.T) {
	t.Parallel()

	publishFailure := errors.New("transport unavailable")
	releaseFailure := errors.New("database release failed")

	db := &recordingDBTX{
		row:     claimedRecordRow(1),
		execErr: releaseFailure,
	}

	publisher := &recordingPublisher{
		err: publishFailure,
	}

	processed, err := ProcessOne(
		context.Background(),
		db,
		publisher,
		"worker-a",
		time.Date(2026, 9, 3, 12, 5, 0, 0, time.UTC),
		time.Date(2026, 9, 3, 12, 1, 0, 0, time.UTC),
	)

	if !processed {
		t.Fatal("ProcessOne processed = false, want true")
	}

	if !errors.Is(err, releaseFailure) {
		t.Fatalf("ProcessOne error = %v, want wrapped release failure", err)
	}

	if !strings.Contains(err.Error(), publishFailure.Error()) {
		t.Fatalf(
			"ProcessOne error %q does not retain publish failure context",
			err.Error(),
		)
	}
}

func TestProcessOneReportsMarkPublishedFailure(t *testing.T) {
	t.Parallel()

	markFailure := errors.New("database mark failed")

	db := &recordingDBTX{
		row:     claimedRecordRow(1),
		execErr: markFailure,
	}

	publisher := &recordingPublisher{}

	processed, err := ProcessOne(
		context.Background(),
		db,
		publisher,
		"worker-a",
		time.Date(2026, 9, 3, 12, 5, 0, 0, time.UTC),
		time.Date(2026, 9, 3, 12, 1, 0, 0, time.UTC),
	)

	if !processed {
		t.Fatal("ProcessOne processed = false, want true")
	}

	if !errors.Is(err, markFailure) {
		t.Fatalf("ProcessOne error = %v, want wrapped mark failure", err)
	}

	if publisher.calls != 1 {
		t.Fatalf("publisher calls = %d, want 1", publisher.calls)
	}
}

func TestProcessOneRejectsMissingBoundaries(t *testing.T) {
	t.Parallel()

	validDB := &recordingDBTX{}
	validPublisher := &recordingPublisher{}

	var typedNilDB *recordingDBTX
	var typedNilPublisher *recordingPublisher

	leaseUntil := time.Date(2026, 9, 3, 12, 5, 0, 0, time.UTC)
	failureAvailableAt := time.Date(
		2026,
		9,
		3,
		12,
		1,
		0,
		0,
		time.UTC,
	)

	tests := []struct {
		name               string
		db                 databaseDBTXAlias
		publisher          Publisher
		leaseOwner         string
		leaseUntil         time.Time
		failureAvailableAt time.Time
		want               error
	}{
		{
			name:               "nil database",
			db:                 nil,
			publisher:          validPublisher,
			leaseOwner:         "worker-a",
			leaseUntil:         leaseUntil,
			failureAvailableAt: failureAvailableAt,
			want:               ErrDatabaseRequired,
		},
		{
			name:               "typed nil database",
			db:                 typedNilDB,
			publisher:          validPublisher,
			leaseOwner:         "worker-a",
			leaseUntil:         leaseUntil,
			failureAvailableAt: failureAvailableAt,
			want:               ErrDatabaseRequired,
		},
		{
			name:               "nil publisher",
			db:                 validDB,
			publisher:          nil,
			leaseOwner:         "worker-a",
			leaseUntil:         leaseUntil,
			failureAvailableAt: failureAvailableAt,
			want:               ErrPublisherRequired,
		},
		{
			name:               "typed nil publisher",
			db:                 validDB,
			publisher:          typedNilPublisher,
			leaseOwner:         "worker-a",
			leaseUntil:         leaseUntil,
			failureAvailableAt: failureAvailableAt,
			want:               ErrPublisherRequired,
		},
		{
			name:               "blank lease owner",
			db:                 validDB,
			publisher:          validPublisher,
			leaseOwner:         " ",
			leaseUntil:         leaseUntil,
			failureAvailableAt: failureAvailableAt,
			want:               ErrLeaseOwnerRequired,
		},
		{
			name:               "zero lease deadline",
			db:                 validDB,
			publisher:          validPublisher,
			leaseOwner:         "worker-a",
			failureAvailableAt: failureAvailableAt,
			want:               ErrLeaseUntilRequired,
		},
		{
			name:       "zero failure availability",
			db:         validDB,
			publisher:  validPublisher,
			leaseOwner: "worker-a",
			leaseUntil: leaseUntil,
			want:       ErrAvailableAtRequired,
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			processed, err := ProcessOne(
				context.Background(),
				test.db,
				test.publisher,
				test.leaseOwner,
				test.leaseUntil,
				test.failureAvailableAt,
			)

			if processed {
				t.Fatal("ProcessOne processed = true, want false")
			}

			if !errors.Is(err, test.want) {
				t.Fatalf("ProcessOne error = %v, want %v", err, test.want)
			}
		})
	}
}

// databaseDBTXAlias keeps the table-driven validation cases explicit while
// using the package's production DBTX contract.
type databaseDBTXAlias interface {
	Exec(
		context.Context,
		string,
		...any,
	) (pgconn.CommandTag, error)
	Query(
		context.Context,
		string,
		...any,
	) (pgx.Rows, error)
	QueryRow(
		context.Context,
		string,
		...any,
	) pgx.Row
}
