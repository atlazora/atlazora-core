package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type recordingDBTX struct {
	execCalls int
	sql       string
	args      []any
	execErr   error
	execTag   pgconn.CommandTag

	queryRowCalls int
	queryRowSQL   string
	queryRowArgs  []any
	row           pgx.Row
}

func (db *recordingDBTX) Exec(
	_ context.Context,
	sql string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	db.execCalls++
	db.sql = sql
	db.args = append([]any(nil), arguments...)

	if db.execErr != nil {
		return pgconn.CommandTag{}, db.execErr
	}

	if db.execTag.String() != "" {
		return db.execTag, nil
	}

	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (*recordingDBTX) Query(
	context.Context,
	string,
	...any,
) (pgx.Rows, error) {
	panic("Query must not be called")
}

func (db *recordingDBTX) QueryRow(
	_ context.Context,
	sql string,
	arguments ...any,
) pgx.Row {
	db.queryRowCalls++
	db.queryRowSQL = sql
	db.queryRowArgs = append([]any(nil), arguments...)

	if db.row == nil {
		return stubRow{err: pgx.ErrNoRows}
	}

	return db.row
}

type stubRow struct {
	values []any
	err    error
}

func (row stubRow) Scan(dest ...any) error {
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
		case *string:
			*target = value.(string)
		case *time.Time:
			*target = value.(time.Time)
		case *[]byte:
			*target = append((*target)[:0], value.([]byte)...)
		case *int:
			*target = value.(int)
		default:
			return errors.New("unsupported scan destination")
		}
	}

	return nil
}

func validEvent() Event {
	return Event{
		ID:         "018f47a2-7b3c-7abc-8def-0123456789ab",
		Source:     "atlazora://core/test",
		Type:       "com.atlazora.test.created.v1",
		Time:       time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
		DataSchema: "https://contracts.atlazora.test/events/test-created/v1",
		Envelope: json.RawMessage(
			`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-0123456789ab"}`,
		),
	}
}

func TestEnqueueWritesRequiredOutboxFields(t *testing.T) {
	t.Parallel()

	db := &recordingDBTX{}
	event := validEvent()

	if err := Enqueue(context.Background(), db, event); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if db.execCalls != 1 {
		t.Fatalf("expected one Exec call, got %d", db.execCalls)
	}

	requiredSQL := []string{
		"INSERT INTO atlazora_outbox",
		"event_id",
		"event_source",
		"event_type",
		"event_time",
		"data_schema",
		"envelope",
		"$6::jsonb",
	}

	for _, fragment := range requiredSQL {
		if !strings.Contains(db.sql, fragment) {
			t.Fatalf("SQL missing required fragment %q", fragment)
		}
	}

	if len(db.args) != 6 {
		t.Fatalf("expected 6 SQL arguments, got %d", len(db.args))
	}
}

func TestEnqueueRejectsInvalidInputBeforeDatabaseWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Event)
		want   error
	}{
		{
			name: "blank event id",
			mutate: func(event *Event) {
				event.ID = " "
			},
			want: ErrEventIDRequired,
		},
		{
			name: "blank source",
			mutate: func(event *Event) {
				event.Source = " "
			},
			want: ErrEventSourceRequired,
		},
		{
			name: "blank type",
			mutate: func(event *Event) {
				event.Type = ""
			},
			want: ErrEventTypeRequired,
		},
		{
			name: "zero time",
			mutate: func(event *Event) {
				event.Time = time.Time{}
			},
			want: ErrEventTimeRequired,
		},
		{
			name: "blank data schema",
			mutate: func(event *Event) {
				event.DataSchema = " "
			},
			want: ErrDataSchemaRequired,
		},
		{
			name: "empty envelope",
			mutate: func(event *Event) {
				event.Envelope = nil
			},
			want: ErrEnvelopeRequired,
		},
		{
			name: "invalid JSON envelope",
			mutate: func(event *Event) {
				event.Envelope = json.RawMessage(`{"broken":`)
			},
			want: ErrEnvelopeInvalid,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := &recordingDBTX{}
			event := validEvent()
			tt.mutate(&event)

			err := Enqueue(context.Background(), db, event)

			if !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, err)
			}

			if db.execCalls != 0 {
				t.Fatalf("invalid event reached database; Exec calls=%d", db.execCalls)
			}
		})
	}
}

func TestClaimNextReturnsClaimedRecord(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	leaseUntil := eventTime.Add(5 * time.Minute)
	envelope := []byte(`{"id":"018f47a2-7b3c-7abc-8def-0123456789ab"}`)

	db := &recordingDBTX{
		row: stubRow{
			values: []any{
				int64(42),
				"018f47a2-7b3c-7abc-8def-0123456789ab",
				"atlazora://core/test",
				"com.atlazora.test.created.v1",
				eventTime,
				"https://contracts.atlazora.test/events/test-created/v1",
				envelope,
				2,
			},
		},
	}

	record, found, err := ClaimNext(
		context.Background(),
		db,
		"worker-a",
		leaseUntil,
	)
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	if !found {
		t.Fatal("expected claimed record")
	}

	if record.OutboxID != 42 {
		t.Fatalf("unexpected outbox id: %d", record.OutboxID)
	}

	if record.EventID != "018f47a2-7b3c-7abc-8def-0123456789ab" {
		t.Fatalf("unexpected event id: %s", record.EventID)
	}

	if record.AttemptCount != 2 {
		t.Fatalf("unexpected attempt count: %d", record.AttemptCount)
	}

	if string(record.Envelope) != string(envelope) {
		t.Fatalf("unexpected envelope: %s", record.Envelope)
	}

	requiredSQL := []string{
		"FOR UPDATE SKIP LOCKED",
		"state = 'pending'",
		"available_at <= CURRENT_TIMESTAMP",
		"state = 'processing'",
		"attempt_count = attempt_count + 1",
		"lease_owner = $1",
		"lease_until = $2",
		"RETURNING",
	}

	for _, fragment := range requiredSQL {
		if !strings.Contains(db.queryRowSQL, fragment) {
			t.Fatalf("claim SQL missing %q", fragment)
		}
	}
}

func TestClaimNextRecoversExpiredLease(t *testing.T) {
	t.Parallel()

	leaseUntil := time.Date(2026, 9, 3, 12, 5, 0, 0, time.UTC)

	db := &recordingDBTX{
		row: stubRow{
			values: []any{
				int64(42),
				"018f47a2-7b3c-7abc-8def-0123456789ab",
				"atlazora://core/test",
				"com.atlazora.test.created.v1",
				time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
				"https://contracts.atlazora.test/events/test-created/v1",
				[]byte(`{"id":"018f47a2-7b3c-7abc-8def-0123456789ab"}`),
				2,
			},
		},
	}

	record, found, err := ClaimNext(
		context.Background(),
		db,
		"recovery-worker",
		leaseUntil,
	)
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	if !found {
		t.Fatal("expected expired processing lease to be claimable")
	}

	if record.AttemptCount != 2 {
		t.Fatalf("recovered attempt count = %d, want 2", record.AttemptCount)
	}

	requiredSQL := []string{
		"state = 'pending'",
		"available_at <= CURRENT_TIMESTAMP",
		"state = 'processing'",
		"lease_until <= CURRENT_TIMESTAMP",
		"FOR UPDATE SKIP LOCKED",
		"attempt_count = attempt_count + 1",
	}

	for _, fragment := range requiredSQL {
		if !strings.Contains(db.queryRowSQL, fragment) {
			t.Fatalf("expired lease claim SQL missing %q", fragment)
		}
	}
}
func TestClaimNextReturnsNotFoundWithoutError(t *testing.T) {
	t.Parallel()

	db := &recordingDBTX{
		row: stubRow{err: pgx.ErrNoRows},
	}

	record, found, err := ClaimNext(
		context.Background(),
		db,
		"worker-a",
		time.Now().UTC().Add(time.Minute),
	)

	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	if found {
		t.Fatalf("expected no record, got %#v", record)
	}
}

func TestClaimNextRejectsMissingLeaseInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		db         *recordingDBTX
		leaseOwner string
		leaseUntil time.Time
		want       error
	}{
		{
			name:       "missing database",
			db:         nil,
			leaseOwner: "worker-a",
			leaseUntil: time.Now().UTC(),
			want:       ErrDatabaseRequired,
		},
		{
			name:       "blank lease owner",
			db:         &recordingDBTX{},
			leaseOwner: " ",
			leaseUntil: time.Now().UTC(),
			want:       ErrLeaseOwnerRequired,
		},
		{
			name:       "zero lease deadline",
			db:         &recordingDBTX{},
			leaseOwner: "worker-a",
			leaseUntil: time.Time{},
			want:       ErrLeaseUntilRequired,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := ClaimNext(
				context.Background(),
				tt.db,
				tt.leaseOwner,
				tt.leaseUntil,
			)

			if !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, err)
			}
		})
	}
}

func TestReleaseFailedReturnsOwnedRecordToPending(t *testing.T) {
	t.Parallel()

	nextAvailable := time.Date(2026, 9, 3, 12, 10, 0, 0, time.UTC)

	db := &recordingDBTX{
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}

	err := ReleaseFailed(
		context.Background(),
		db,
		42,
		"worker-a",
		nextAvailable,
		errors.New("temporary transport failure"),
	)
	if err != nil {
		t.Fatalf("ReleaseFailed returned error: %v", err)
	}

	requiredSQL := []string{
		"state = 'pending'",
		"available_at = $3",
		"lease_owner = NULL",
		"lease_until = NULL",
		"last_error = $4",
		"state = 'processing'",
		"lease_owner = $2",
	}

	for _, fragment := range requiredSQL {
		if !strings.Contains(db.sql, fragment) {
			t.Fatalf("release SQL missing %q", fragment)
		}
	}
}

func TestReleaseFailedRejectsLostOwnership(t *testing.T) {
	t.Parallel()

	db := &recordingDBTX{
		execTag: pgconn.NewCommandTag("UPDATE 0"),
	}

	err := ReleaseFailed(
		context.Background(),
		db,
		42,
		"worker-a",
		time.Now().UTC(),
		errors.New("failure"),
	)

	if !errors.Is(err, ErrOutboxNotOwned) {
		t.Fatalf("expected ErrOutboxNotOwned, got %v", err)
	}
}

func TestMarkPublishedTransitionsOwnedRecord(t *testing.T) {
	t.Parallel()

	db := &recordingDBTX{
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}

	err := MarkPublished(
		context.Background(),
		db,
		42,
		"worker-a",
	)
	if err != nil {
		t.Fatalf("MarkPublished returned error: %v", err)
	}

	requiredSQL := []string{
		"state = 'published'",
		"lease_owner = NULL",
		"lease_until = NULL",
		"last_error = NULL",
		"published_at = CURRENT_TIMESTAMP",
		"state = 'processing'",
		"lease_owner = $2",
	}

	for _, fragment := range requiredSQL {
		if !strings.Contains(db.sql, fragment) {
			t.Fatalf("published SQL missing %q", fragment)
		}
	}
}

func TestMarkPublishedRejectsLostOwnership(t *testing.T) {
	t.Parallel()

	db := &recordingDBTX{
		execTag: pgconn.NewCommandTag("UPDATE 0"),
	}

	err := MarkPublished(
		context.Background(),
		db,
		42,
		"worker-a",
	)

	if !errors.Is(err, ErrOutboxNotOwned) {
		t.Fatalf("expected ErrOutboxNotOwned, got %v", err)
	}
}

func TestEnqueueRejectsNilDatabase(t *testing.T) {
	t.Parallel()

	err := Enqueue(context.Background(), nil, validEvent())

	if !errors.Is(err, ErrDatabaseRequired) {
		t.Fatalf("expected ErrDatabaseRequired, got %v", err)
	}
}

func TestEnqueueReturnsDatabaseFailure(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("database unavailable")
	db := &recordingDBTX{execErr: sentinel}

	err := Enqueue(context.Background(), db, validEvent())

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped database error, got %v", err)
	}

	if db.execCalls != 1 {
		t.Fatalf("expected one Exec attempt, got %d", db.execCalls)
	}
}
