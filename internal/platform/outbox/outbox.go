package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/atlazora/atlazora-core/internal/platform/database"
	"github.com/jackc/pgx/v5"
)

var (
	ErrDatabaseRequired    = errors.New("outbox database executor is required")
	ErrEventIDRequired     = errors.New("event id is required")
	ErrEventSourceRequired = errors.New("event source is required")
	ErrEventTypeRequired   = errors.New("event type is required")
	ErrEventTimeRequired   = errors.New("event time is required")
	ErrDataSchemaRequired  = errors.New("event data schema is required")
	ErrEnvelopeRequired    = errors.New("event envelope is required")
	ErrEnvelopeInvalid     = errors.New("event envelope must be valid JSON")
	ErrLeaseOwnerRequired  = errors.New("outbox lease owner is required")
	ErrLeaseUntilRequired  = errors.New("outbox lease deadline is required")
	ErrAvailableAtRequired = errors.New("outbox next availability is required")
	ErrOutboxNotOwned      = errors.New("outbox record is not owned by caller")
)

// Event is the persistence input required to durably record an already-created
// event for later asynchronous publication.
//
// Shared executable CloudEvents schema semantics remain owned by
// atlazora-contracts. This value only carries the fields required by the
// transport-independent PostgreSQL outbox persistence boundary.
type Event struct {
	ID         string
	Source     string
	Type       string
	Time       time.Time
	DataSchema string
	Envelope   json.RawMessage
}

// Record is one claimed outbox publication record.
//
// EventID remains the CloudEvents identity across retries/redelivery.
// OutboxID is persistence identity only and must not replace EventID.
type Record struct {
	OutboxID     int64
	EventID      string
	EventSource  string
	EventType    string
	EventTime    time.Time
	DataSchema   string
	Envelope     json.RawMessage
	AttemptCount int
}

// dbRequired rejects both a nil interface and an interface containing a
// typed-nil pointer. Without this guard, a typed-nil DBTX can pass a
// direct interface nil check and panic when a method is invoked.
func dbRequired(db database.DBTX) bool {
	if db == nil {
		return false
	}

	value := reflect.ValueOf(db)

	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Ptr,
		reflect.Slice:
		return !value.IsNil()
	default:
		return true
	}
}

// Enqueue writes durable publication intent using the supplied DBTX.
//
// Callers that require atomic authoritative-state and outbox persistence must
// pass the DBTX received from database.WithinTransaction. Enqueue does not
// open, commit, or publish outside that transaction.
func Enqueue(ctx context.Context, db database.DBTX, event Event) error {
	if !dbRequired(db) {
		return ErrDatabaseRequired
	}

	if strings.TrimSpace(event.ID) == "" {
		return ErrEventIDRequired
	}

	if strings.TrimSpace(event.Source) == "" {
		return ErrEventSourceRequired
	}

	if strings.TrimSpace(event.Type) == "" {
		return ErrEventTypeRequired
	}

	if event.Time.IsZero() {
		return ErrEventTimeRequired
	}

	if strings.TrimSpace(event.DataSchema) == "" {
		return ErrDataSchemaRequired
	}

	if len(event.Envelope) == 0 {
		return ErrEnvelopeRequired
	}

	if !json.Valid(event.Envelope) {
		return ErrEnvelopeInvalid
	}

	const statement = `
INSERT INTO atlazora_outbox (
event_id,
event_source,
event_type,
event_time,
data_schema,
envelope
)
VALUES ($1, $2, $3, $4, $5, $6::jsonb)
`

	if _, err := db.Exec(
		ctx,
		statement,
		event.ID,
		event.Source,
		event.Type,
		event.Time,
		event.DataSchema,
		string(event.Envelope),
	); err != nil {
		return fmt.Errorf("enqueue outbox event: %w", err)
	}

	return nil
}

// ClaimNext atomically claims the next currently-available pending outbox row
// or recovers a processing row whose lease has expired.
//
// Concurrent workers skip rows already locked by another claimant.
// AttemptCount increments on each successful claim or expired-lease recovery.
// A lease duration/policy is intentionally not chosen here; the caller provides
// the absolute lease deadline.
func ClaimNext(
	ctx context.Context,
	db database.DBTX,
	leaseOwner string,
	leaseUntil time.Time,
) (Record, bool, error) {
	if !dbRequired(db) {
		return Record{}, false, ErrDatabaseRequired
	}

	if strings.TrimSpace(leaseOwner) == "" {
		return Record{}, false, ErrLeaseOwnerRequired
	}

	if leaseUntil.IsZero() {
		return Record{}, false, ErrLeaseUntilRequired
	}

	const statement = `
WITH candidate AS (
SELECT outbox_id
  FROM atlazora_outbox
 WHERE (
state = 'pending'
AND available_at <= CURRENT_TIMESTAMP
)
    OR (
state = 'processing'
AND lease_until <= CURRENT_TIMESTAMP
)
 ORDER BY available_at, outbox_id
 FOR UPDATE SKIP LOCKED
 LIMIT 1
)
UPDATE atlazora_outbox AS o
   SET state = 'processing',
       attempt_count = attempt_count + 1,
       lease_owner = $1,
       lease_until = $2,
       last_error = NULL
  FROM candidate
 WHERE o.outbox_id = candidate.outbox_id
RETURNING
o.outbox_id,
o.event_id::text,
o.event_source,
o.event_type,
o.event_time,
o.data_schema,
o.envelope,
o.attempt_count
`

	var record Record
	var envelope []byte

	err := db.QueryRow(
		ctx,
		statement,
		leaseOwner,
		leaseUntil,
	).Scan(
		&record.OutboxID,
		&record.EventID,
		&record.EventSource,
		&record.EventType,
		&record.EventTime,
		&record.DataSchema,
		&envelope,
		&record.AttemptCount,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, false, nil
	}

	if err != nil {
		return Record{}, false, fmt.Errorf("claim outbox event: %w", err)
	}

	record.Envelope = append(json.RawMessage(nil), envelope...)

	return record, true, nil
}

// ReleaseFailed returns one caller-owned processing record to pending.
//
// No retry/backoff algorithm is selected here. The caller supplies the next
// absolute availability time.
func ReleaseFailed(
	ctx context.Context,
	db database.DBTX,
	outboxID int64,
	leaseOwner string,
	availableAt time.Time,
	failure error,
) error {
	if !dbRequired(db) {
		return ErrDatabaseRequired
	}

	if strings.TrimSpace(leaseOwner) == "" {
		return ErrLeaseOwnerRequired
	}

	if availableAt.IsZero() {
		return ErrAvailableAtRequired
	}

	var lastError string
	if failure != nil {
		lastError = failure.Error()
	}

	const statement = `
UPDATE atlazora_outbox
   SET state = 'pending',
       available_at = $3,
       lease_owner = NULL,
       lease_until = NULL,
       last_error = $4
 WHERE outbox_id = $1
   AND state = 'processing'
   AND lease_owner = $2
`

	tag, err := db.Exec(
		ctx,
		statement,
		outboxID,
		leaseOwner,
		availableAt,
		lastError,
	)
	if err != nil {
		return fmt.Errorf("release failed outbox event: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return ErrOutboxNotOwned
	}

	return nil
}

// MarkPublished marks one caller-owned processing record as published.
//
// Publication transport is outside this package. This method only persists the
// successful lifecycle transition after the transport confirms publication.
func MarkPublished(
	ctx context.Context,
	db database.DBTX,
	outboxID int64,
	leaseOwner string,
) error {
	if !dbRequired(db) {
		return ErrDatabaseRequired
	}

	if strings.TrimSpace(leaseOwner) == "" {
		return ErrLeaseOwnerRequired
	}

	const statement = `
UPDATE atlazora_outbox
   SET state = 'published',
       lease_owner = NULL,
       lease_until = NULL,
       last_error = NULL,
       published_at = CURRENT_TIMESTAMP
 WHERE outbox_id = $1
   AND state = 'processing'
   AND lease_owner = $2
`

	tag, err := db.Exec(
		ctx,
		statement,
		outboxID,
		leaseOwner,
	)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return ErrOutboxNotOwned
	}

	return nil
}
