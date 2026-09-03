package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/atlazora/atlazora-core/internal/platform/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func canonicalJSONForTest(t *testing.T, value []byte) []byte {
	t.Helper()

	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		t.Fatalf("decode JSON for semantic comparison: %v", err)
	}

	canonical, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("canonicalize JSON for semantic comparison: %v", err)
	}

	return canonical
}

func assertJSONSemanticallyEqual(t *testing.T, got, want []byte) {
	t.Helper()

	gotCanonical := canonicalJSONForTest(t, got)
	wantCanonical := canonicalJSONForTest(t, want)

	if string(gotCanonical) != string(wantCanonical) {
		t.Fatalf(
			"JSON values differ semantically:\n got: %s\nwant: %s",
			gotCanonical,
			wantCanonical,
		)
	}
}

type postgresIntegrationPublisher struct {
	records []Record
	err     error
}

func (p *postgresIntegrationPublisher) Publish(_ context.Context, record Record) error {
	p.records = append(p.records, record)
	return p.err
}

func TestPostgresTransactionalOutboxIntegration(t *testing.T) {
	databaseURL := os.Getenv("ATLAZORA_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ATLAZORA_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse PostgreSQL integration configuration: %v", err)
	}

	// The version-controlled migration is intentionally executed as one SQL
	// document. Simple protocol permits the existing multi-statement migration
	// without selecting a migration framework.
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open PostgreSQL integration pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL integration database: %v", err)
	}

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test source path")
	}

	migrationPath := filepath.Join(
		filepath.Dir(currentFile),
		"..",
		"database",
		"migrations",
		"0001_wu05_event_outbox_foundation.sql",
	)

	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read WU05 migration: %v", err)
	}

	schemaName := fmt.Sprintf("wu05_outbox_it_%d", time.Now().UnixNano())
	schemaIdentifier := pgx.Identifier{schemaName}.Sanitize()

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		if _, cleanupErr := pool.Exec(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+schemaIdentifier+" CASCADE",
		); cleanupErr != nil {
			t.Errorf("drop integration schema: %v", cleanupErr)
		}
	}()

	eventA := Event{
		ID:         "018f47a2-7b3c-7abc-8def-0123456789ab",
		Source:     "atlazora://core/wu05-integration",
		Type:       "com.atlazora.wu05.integration.created.v1",
		Time:       time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
		DataSchema: "https://contracts.atlazora.test/events/wu05-integration/v1",
		Envelope: []byte(
			`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-0123456789ab","source":"atlazora://core/wu05-integration","type":"com.atlazora.wu05.integration.created.v1","time":"2026-09-03T12:00:00Z","datacontenttype":"application/json","dataschema":"https://contracts.atlazora.test/events/wu05-integration/v1","data":{"probe":"commit"}}`,
		),
	}

	// Commit proof: authoritative write and durable outbox intent share the
	// database.WithinTransaction DBTX and become visible together.
	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "CREATE SCHEMA "+schemaIdentifier); err != nil {
			return fmt.Errorf("create integration schema: %w", err)
		}

		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return fmt.Errorf("set integration search path: %w", err)
		}

		if _, err := tx.Exec(ctx, string(migration)); err != nil {
			return fmt.Errorf("apply WU05 migration: %w", err)
		}

		if _, err := tx.Exec(ctx, `
CREATE TABLE authoritative_probe (
probe_id text PRIMARY KEY,
value text NOT NULL
)`); err != nil {
			return fmt.Errorf("create authoritative probe: %w", err)
		}

		if _, err := tx.Exec(
			ctx,
			"INSERT INTO authoritative_probe (probe_id, value) VALUES ($1, $2)",
			"commit-probe",
			"committed",
		); err != nil {
			return fmt.Errorf("write authoritative state: %w", err)
		}

		if err := Enqueue(ctx, tx, eventA); err != nil {
			return fmt.Errorf("enqueue committed event: %w", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("committed transactional outbox write: %v", err)
	}

	var authoritativeCount int
	if err := pool.QueryRow(
		ctx,
		"SELECT count(*) FROM "+schemaIdentifier+".authoritative_probe",
	).Scan(&authoritativeCount); err != nil {
		t.Fatalf("read committed authoritative probe: %v", err)
	}

	var outboxCount int
	if err := pool.QueryRow(
		ctx,
		"SELECT count(*) FROM "+schemaIdentifier+".atlazora_outbox",
	).Scan(&outboxCount); err != nil {
		t.Fatalf("read committed outbox rows: %v", err)
	}

	if authoritativeCount != 1 || outboxCount != 1 {
		t.Fatalf(
			"atomic commit counts = authoritative:%d outbox:%d, want 1/1",
			authoritativeCount,
			outboxCount,
		)
	}

	// Rollback proof: a callback error removes both the authoritative write and
	// the matching outbox intent.
	rollbackSentinel := errors.New("force transactional rollback")

	eventB := eventA
	eventB.ID = "018f47a2-7b3c-7abc-8def-1123456789ab"
	eventB.Envelope = []byte(
		`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-1123456789ab","source":"atlazora://core/wu05-integration","type":"com.atlazora.wu05.integration.created.v1","time":"2026-09-03T12:00:00Z","datacontenttype":"application/json","dataschema":"https://contracts.atlazora.test/events/wu05-integration/v1","data":{"probe":"rollback"}}`,
	)

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		if _, err := tx.Exec(
			ctx,
			"INSERT INTO authoritative_probe (probe_id, value) VALUES ($1, $2)",
			"rollback-probe",
			"must-not-persist",
		); err != nil {
			return err
		}

		if err := Enqueue(ctx, tx, eventB); err != nil {
			return err
		}

		return rollbackSentinel
	})

	if !errors.Is(err, rollbackSentinel) {
		t.Fatalf("rollback error = %v, want sentinel", err)
	}

	if err := pool.QueryRow(
		ctx,
		"SELECT count(*) FROM "+schemaIdentifier+".authoritative_probe",
	).Scan(&authoritativeCount); err != nil {
		t.Fatalf("read authoritative count after rollback: %v", err)
	}

	if err := pool.QueryRow(
		ctx,
		"SELECT count(*) FROM "+schemaIdentifier+".atlazora_outbox",
	).Scan(&outboxCount); err != nil {
		t.Fatalf("read outbox count after rollback: %v", err)
	}

	if authoritativeCount != 1 || outboxCount != 1 {
		t.Fatalf(
			"rollback counts = authoritative:%d outbox:%d, want 1/1",
			authoritativeCount,
			outboxCount,
		)
	}

	worker := "wu05-integration-worker"
	firstLeaseUntil := time.Now().UTC().Add(5 * time.Minute)

	var firstClaim Record

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		record, found, err := ClaimNext(ctx, tx, worker, firstLeaseUntil)
		if err != nil {
			return err
		}

		if !found {
			return errors.New("expected pending outbox event")
		}

		firstClaim = record
		return nil
	})
	if err != nil {
		t.Fatalf("claim committed outbox event: %v", err)
	}

	if firstClaim.EventID != eventA.ID {
		t.Fatalf("first claim event id = %q, want %q", firstClaim.EventID, eventA.ID)
	}

	if firstClaim.AttemptCount != 1 {
		t.Fatalf("first claim attempt count = %d, want 1", firstClaim.AttemptCount)
	}

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		return ReleaseFailed(
			ctx,
			tx,
			firstClaim.OutboxID,
			worker,
			time.Now().UTC(),
			errors.New("simulated transport failure"),
		)
	})
	if err != nil {
		t.Fatalf("release failed publication: %v", err)
	}

	var retryClaim Record

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		record, found, err := ClaimNext(
			ctx,
			tx,
			worker,
			time.Now().UTC().Add(5*time.Minute),
		)
		if err != nil {
			return err
		}

		if !found {
			return errors.New("expected retryable outbox event")
		}

		retryClaim = record

		return MarkPublished(ctx, tx, record.OutboxID, worker)
	})
	if err != nil {
		t.Fatalf("retry and publish outbox event: %v", err)
	}

	if retryClaim.EventID != eventA.ID {
		t.Fatalf(
			"retry changed CloudEvents identity: got %q want %q",
			retryClaim.EventID,
			eventA.ID,
		)
	}

	if retryClaim.AttemptCount != 2 {
		t.Fatalf("retry attempt count = %d, want 2", retryClaim.AttemptCount)
	}

	var (
		state        string
		attemptCount int
		publishedAt  *time.Time
	)

	if err := pool.QueryRow(
		ctx,
		`SELECT state, attempt_count, published_at
   FROM `+schemaIdentifier+`.atlazora_outbox
  WHERE event_source = $1
    AND event_id = $2`,
		eventA.Source,
		eventA.ID,
	).Scan(
		&state,
		&attemptCount,
		&publishedAt,
	); err != nil {
		t.Fatalf("read published outbox state: %v", err)
	}

	if state != "published" {
		t.Fatalf("final outbox state = %q, want published", state)
	}

	if attemptCount != 2 {
		t.Fatalf("final attempt count = %d, want 2", attemptCount)
	}
	if publishedAt == nil {
		t.Fatal("published_at is nil")
	}

	// Crash/recovery proof: an outbox record left in processing becomes
	// claimable again after its lease expires. The CloudEvents identity is
	// preserved; recovery is a delivery attempt, not a distinct event.
	eventC := eventA
	eventC.ID = "018f47a2-7b3c-7abc-8def-2123456789ab"
	eventC.Envelope = []byte(
		`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-2123456789ab","source":"atlazora://core/wu05-integration","type":"com.atlazora.wu05.integration.created.v1","time":"2026-09-03T12:00:00Z","datacontenttype":"application/json","dataschema":"https://contracts.atlazora.test/events/wu05-integration/v1","data":{"probe":"expired-lease-probe"}}`,
	)

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		return Enqueue(ctx, tx, eventC)
	})
	if err != nil {
		t.Fatalf("enqueue expired-lease probe: %v", err)
	}

	var crashedClaim Record

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		record, found, err := ClaimNext(
			ctx,
			tx,
			"crashed-worker",
			time.Now().UTC().Add(-time.Second),
		)
		if err != nil {
			return err
		}

		if !found {
			return errors.New("expected expired-lease probe initial claim")
		}

		crashedClaim = record
		return nil
	})
	if err != nil {
		t.Fatalf("create expired processing lease: %v", err)
	}

	if crashedClaim.EventID != eventC.ID {
		t.Fatalf(
			"initial expired-lease probe event id = %q, want %q",
			crashedClaim.EventID,
			eventC.ID,
		)
	}

	if crashedClaim.AttemptCount != 1 {
		t.Fatalf(
			"initial expired-lease attempt count = %d, want 1",
			crashedClaim.AttemptCount,
		)
	}

	var recoveredClaim Record

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		record, found, err := ClaimNext(
			ctx,
			tx,
			"recovery-worker",
			time.Now().UTC().Add(5*time.Minute),
		)
		if err != nil {
			return err
		}

		if !found {
			return errors.New("expected expired processing lease recovery")
		}

		recoveredClaim = record

		return MarkPublished(
			ctx,
			tx,
			record.OutboxID,
			"recovery-worker",
		)
	})
	if err != nil {
		t.Fatalf("recover expired processing lease: %v", err)
	}

	if recoveredClaim.EventID != eventC.ID {
		t.Fatalf(
			"expired lease recovery changed event id: got %q want %q",
			recoveredClaim.EventID,
			eventC.ID,
		)
	}

	if recoveredClaim.AttemptCount != 2 {
		t.Fatalf(
			"expired lease recovery attempt count = %d, want 2",
			recoveredClaim.AttemptCount,
		)
	}

	var (
		recoveredState    string
		recoveredAttempts int
		recoveredOwner    *string
	)

	if err := pool.QueryRow(
		ctx,
		`SELECT state, attempt_count, lease_owner
   FROM `+schemaIdentifier+`.atlazora_outbox
  WHERE event_source = $1
    AND event_id = $2`,
		eventC.Source,
		eventC.ID,
	).Scan(
		&recoveredState,
		&recoveredAttempts,
		&recoveredOwner,
	); err != nil {
		t.Fatalf("read expired-lease recovery state: %v", err)
	}

	if recoveredState != "published" {
		t.Fatalf("recovered state = %q, want published", recoveredState)
	}

	if recoveredAttempts != 2 {
		t.Fatalf("recovered attempt count = %d, want 2", recoveredAttempts)
	}

	if recoveredOwner != nil {
		t.Fatalf("published recovered record retained lease owner %q", *recoveredOwner)
	}

	// Idempotent-consumption proof: the first delivery records the
	// authoritative business mutation and the consumption marker in the same
	// PostgreSQL transaction.
	consumerName := "wu05.integration.consumer"
	consumedEventSource := eventA.Source
	consumedEventID := eventA.ID

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		recorded, err := RecordConsumption(
			ctx,
			tx,
			consumerName,
			consumedEventSource,
			consumedEventID,
		)
		if err != nil {
			return err
		}

		if !recorded {
			return errors.New("expected first consumption marker")
		}

		if _, err := tx.Exec(
			ctx,
			"INSERT INTO authoritative_probe (probe_id, value) VALUES ($1, $2)",
			"idempotency-business-first",
			"processed-once",
		); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("commit first idempotent consumption: %v", err)
	}

	var consumptionCount int
	if err := pool.QueryRow(
		ctx,
		`SELECT count(*)
   FROM `+schemaIdentifier+`.atlazora_event_consumption
  WHERE consumer_name = $1
    AND event_source = $2
    AND event_id = $3`,
		consumerName,
		consumedEventSource,
		consumedEventID,
	).Scan(&consumptionCount); err != nil {
		t.Fatalf("read first consumption marker: %v", err)
	}

	if consumptionCount != 1 {
		t.Fatalf("first consumption marker count = %d, want 1", consumptionCount)
	}

	var idempotentBusinessCount int
	if err := pool.QueryRow(
		ctx,
		`SELECT count(*)
   FROM `+schemaIdentifier+`.authoritative_probe
  WHERE probe_id = $1`,
		"idempotency-business-first",
	).Scan(&idempotentBusinessCount); err != nil {
		t.Fatalf("read first idempotent business mutation: %v", err)
	}

	if idempotentBusinessCount != 1 {
		t.Fatalf(
			"first idempotent business mutation count = %d, want 1",
			idempotentBusinessCount,
		)
	}

	// Duplicate delivery: the existing marker must suppress the business
	// mutation for the same logical consumer + source + event id.

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		recorded, err := RecordConsumption(
			ctx,
			tx,
			consumerName,
			consumedEventSource,
			consumedEventID,
		)
		if err != nil {
			return err
		}

		if recorded {
			return errors.New("duplicate delivery unexpectedly acquired consumption marker")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("duplicate idempotent consumption: %v", err)
	}

	if err := pool.QueryRow(
		ctx,
		`SELECT count(*)
   FROM `+schemaIdentifier+`.atlazora_event_consumption
  WHERE consumer_name = $1
    AND event_source = $2
    AND event_id = $3`,
		consumerName,
		consumedEventSource,
		consumedEventID,
	).Scan(&consumptionCount); err != nil {
		t.Fatalf("read marker after duplicate delivery: %v", err)
	}

	if consumptionCount != 1 {
		t.Fatalf("marker count after duplicate = %d, want 1", consumptionCount)
	}

	if err := pool.QueryRow(
		ctx,
		`SELECT count(*)
   FROM `+schemaIdentifier+`.authoritative_probe
  WHERE probe_id = $1`,
		"idempotency-business-first",
	).Scan(&idempotentBusinessCount); err != nil {
		t.Fatalf("read business state after duplicate delivery: %v", err)
	}

	if idempotentBusinessCount != 1 {
		t.Fatalf(
			"business mutation count after duplicate = %d, want 1",
			idempotentBusinessCount,
		)
	}

	// Same CloudEvent identity is independently processable by another logical
	// consumer because consumer_name is part of the duplicate boundary.
	secondConsumerName := "wu05.integration.second-consumer"

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		recorded, err := RecordConsumption(
			ctx,
			tx,
			secondConsumerName,
			consumedEventSource,
			consumedEventID,
		)
		if err != nil {
			return err
		}

		if !recorded {
			return errors.New("different logical consumer was treated as duplicate")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("record event for second logical consumer: %v", err)
	}

	var allConsumerCount int
	if err := pool.QueryRow(
		ctx,
		`SELECT count(*)
   FROM `+schemaIdentifier+`.atlazora_event_consumption
  WHERE event_source = $1
    AND event_id = $2`,
		consumedEventSource,
		consumedEventID,
	).Scan(&allConsumerCount); err != nil {
		t.Fatalf("read logical-consumer isolation markers: %v", err)
	}

	if allConsumerCount != 2 {
		t.Fatalf(
			"same event marker count across consumers = %d, want 2",
			allConsumerCount,
		)
	}

	// Transaction rollback must remove both the newly acquired consumption
	// marker and the authoritative business mutation.
	rollbackConsumerName := "wu05.integration.rollback-consumer"
	rollbackConsumptionSentinel := errors.New("force idempotent consumption rollback")

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		recorded, err := RecordConsumption(
			ctx,
			tx,
			rollbackConsumerName,
			consumedEventSource,
			consumedEventID,
		)
		if err != nil {
			return err
		}

		if !recorded {
			return errors.New("expected rollback consumption marker acquisition")
		}

		if _, err := tx.Exec(
			ctx,
			"INSERT INTO authoritative_probe (probe_id, value) VALUES ($1, $2)",
			"idempotency-business-rollback",
			"must-not-persist",
		); err != nil {
			return err
		}

		return rollbackConsumptionSentinel
	})

	if !errors.Is(err, rollbackConsumptionSentinel) {
		t.Fatalf(
			"idempotent consumption rollback error = %v, want sentinel",
			err,
		)
	}

	var rollbackMarkerCount int
	if err := pool.QueryRow(
		ctx,
		`SELECT count(*)
   FROM `+schemaIdentifier+`.atlazora_event_consumption
  WHERE consumer_name = $1
    AND event_source = $2
    AND event_id = $3`,
		rollbackConsumerName,
		consumedEventSource,
		consumedEventID,
	).Scan(&rollbackMarkerCount); err != nil {
		t.Fatalf("read rolled-back consumption marker: %v", err)
	}

	if rollbackMarkerCount != 0 {
		t.Fatalf(
			"rolled-back consumption marker count = %d, want 0",
			rollbackMarkerCount,
		)
	}

	var rollbackBusinessCount int
	if err := pool.QueryRow(
		ctx,
		`SELECT count(*)
   FROM `+schemaIdentifier+`.authoritative_probe
  WHERE probe_id = $1`,
		"idempotency-business-rollback",
	).Scan(&rollbackBusinessCount); err != nil {
		t.Fatalf("read rolled-back business mutation: %v", err)
	}

	if rollbackBusinessCount != 0 {
		t.Fatalf(
			"rolled-back business mutation count = %d, want 0",
			rollbackBusinessCount,
		)
	}

	// ProcessOne success proof: exercise the transport-independent processor
	// against real PostgreSQL without selecting a broker or holding a database
	// transaction open across publication.
	processorSuccessEvent := eventA
	processorSuccessEvent.ID = "018f47a2-7b3c-7abc-8def-3123456789ab"
	processorSuccessEvent.Envelope = []byte(
		`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-3123456789ab","source":"atlazora://core/wu05-integration","type":"com.atlazora.wu05.integration.created.v1","time":"2026-09-03T12:00:00Z","datacontenttype":"application/json","dataschema":"https://contracts.atlazora.test/events/wu05-integration/v1","data":{"probe":"processor-success-event"}}`,
	)

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		return Enqueue(ctx, tx, processorSuccessEvent)
	})
	if err != nil {
		t.Fatalf("enqueue processor success event: %v", err)
	}

	processorConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire processor integration connection: %v", err)
	}
	defer func() {
		_, _ = processorConn.Exec(context.Background(), "RESET search_path")
		processorConn.Release()
	}()

	if _, err := processorConn.Exec(
		ctx,
		"SET search_path TO "+schemaIdentifier,
	); err != nil {
		t.Fatalf("set processor integration search path: %v", err)
	}

	successPublisher := &postgresIntegrationPublisher{}

	processed, err := ProcessOne(
		ctx,
		processorConn,
		successPublisher,
		"wu05-process-one-success",
		time.Now().UTC().Add(5*time.Minute),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("ProcessOne success path: %v", err)
	}

	if !processed {
		t.Fatal("ProcessOne success path processed = false, want true")
	}

	if len(successPublisher.records) != 1 {
		t.Fatalf(
			"success publisher records = %d, want 1",
			len(successPublisher.records),
		)
	}

	successRecord := successPublisher.records[0]

	if successRecord.EventID != processorSuccessEvent.ID {
		t.Fatalf(
			"ProcessOne success event id = %q, want %q",
			successRecord.EventID,
			processorSuccessEvent.ID,
		)
	}

	if successRecord.EventSource != processorSuccessEvent.Source {
		t.Fatalf(
			"ProcessOne success event source = %q, want %q",
			successRecord.EventSource,
			processorSuccessEvent.Source,
		)
	}

	assertJSONSemanticallyEqual(
		t,
		successRecord.Envelope,
		processorSuccessEvent.Envelope,
	)

	if successRecord.AttemptCount != 1 {
		t.Fatalf(
			"ProcessOne success attempt count = %d, want 1",
			successRecord.AttemptCount,
		)
	}

	var (
		processorSuccessState    string
		processorSuccessAttempts int
		processorSuccessOwner    *string
		processorSuccessAt       *time.Time
	)

	if err := pool.QueryRow(
		ctx,
		`SELECT state, attempt_count, lease_owner, published_at
   FROM `+schemaIdentifier+`.atlazora_outbox
  WHERE event_source = $1
    AND event_id = $2`,
		processorSuccessEvent.Source,
		processorSuccessEvent.ID,
	).Scan(
		&processorSuccessState,
		&processorSuccessAttempts,
		&processorSuccessOwner,
		&processorSuccessAt,
	); err != nil {
		t.Fatalf("read ProcessOne success state: %v", err)
	}

	if processorSuccessState != "published" {
		t.Fatalf(
			"ProcessOne success final state = %q, want published",
			processorSuccessState,
		)
	}

	if processorSuccessAttempts != 1 {
		t.Fatalf(
			"ProcessOne success final attempts = %d, want 1",
			processorSuccessAttempts,
		)
	}

	if processorSuccessOwner != nil {
		t.Fatalf(
			"ProcessOne success retained lease owner %q",
			*processorSuccessOwner,
		)
	}

	if processorSuccessAt == nil {
		t.Fatal("ProcessOne success published_at is nil")
	}

	// Failure/retry proof: a transport error returns the claimed event to
	// pending using the caller-supplied availability timestamp. A later retry
	// republishes the same CloudEvents identity and succeeds on attempt two.
	processorFailureEvent := eventA
	processorFailureEvent.ID = "018f47a2-7b3c-7abc-8def-4123456789ab"
	processorFailureEvent.Envelope = []byte(
		`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-4123456789ab","source":"atlazora://core/wu05-integration","type":"com.atlazora.wu05.integration.created.v1","time":"2026-09-03T12:00:00Z","datacontenttype":"application/json","dataschema":"https://contracts.atlazora.test/events/wu05-integration/v1","data":{"probe":"processor-failure-event"}}`,
	)

	err = database.WithinTransaction(ctx, pool, func(ctx context.Context, tx database.DBTX) error {
		if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+schemaIdentifier); err != nil {
			return err
		}

		return Enqueue(ctx, tx, processorFailureEvent)
	})
	if err != nil {
		t.Fatalf("enqueue processor failure event: %v", err)
	}

	transportFailure := errors.New("integration transport unavailable")
	failingPublisher := &postgresIntegrationPublisher{
		err: transportFailure,
	}

	failureAvailableAt := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)

	processed, err = ProcessOne(
		ctx,
		processorConn,
		failingPublisher,
		"wu05-process-one-failure",
		time.Now().UTC().Add(5*time.Minute),
		failureAvailableAt,
	)

	if !processed {
		t.Fatal("ProcessOne failure path processed = false, want true")
	}

	if !errors.Is(err, transportFailure) {
		t.Fatalf(
			"ProcessOne failure error = %v, want wrapped transport failure",
			err,
		)
	}

	if len(failingPublisher.records) != 1 {
		t.Fatalf(
			"failing publisher records = %d, want 1",
			len(failingPublisher.records),
		)
	}

	failedRecord := failingPublisher.records[0]

	if failedRecord.EventID != processorFailureEvent.ID {
		t.Fatalf(
			"failed publication event id = %q, want %q",
			failedRecord.EventID,
			processorFailureEvent.ID,
		)
	}

	if failedRecord.EventSource != processorFailureEvent.Source {
		t.Fatalf(
			"failed publication event source = %q, want %q",
			failedRecord.EventSource,
			processorFailureEvent.Source,
		)
	}

	assertJSONSemanticallyEqual(
		t,
		failedRecord.Envelope,
		processorFailureEvent.Envelope,
	)

	if failedRecord.AttemptCount != 1 {
		t.Fatalf(
			"failed publication attempt count = %d, want 1",
			failedRecord.AttemptCount,
		)
	}

	var (
		failedState       string
		failedAttempts    int
		failedOwner       *string
		failedLastError   *string
		failedAvailableAt time.Time
	)

	if err := pool.QueryRow(
		ctx,
		`SELECT state, attempt_count, lease_owner, last_error, available_at
   FROM `+schemaIdentifier+`.atlazora_outbox
  WHERE event_source = $1
    AND event_id = $2`,
		processorFailureEvent.Source,
		processorFailureEvent.ID,
	).Scan(
		&failedState,
		&failedAttempts,
		&failedOwner,
		&failedLastError,
		&failedAvailableAt,
	); err != nil {
		t.Fatalf("read ProcessOne failed state: %v", err)
	}

	if failedState != "pending" {
		t.Fatalf(
			"ProcessOne failure state = %q, want pending",
			failedState,
		)
	}

	if failedAttempts != 1 {
		t.Fatalf(
			"ProcessOne failure attempts = %d, want 1",
			failedAttempts,
		)
	}

	if failedOwner != nil {
		t.Fatalf(
			"ProcessOne failure retained lease owner %q",
			*failedOwner,
		)
	}

	if failedLastError == nil || *failedLastError != transportFailure.Error() {
		t.Fatalf(
			"ProcessOne failure last_error = %v, want %q",
			failedLastError,
			transportFailure.Error(),
		)
	}

	if !failedAvailableAt.Equal(failureAvailableAt) {
		t.Fatalf(
			"ProcessOne failure available_at = %s, want %s",
			failedAvailableAt,
			failureAvailableAt,
		)
	}

	retryPublisher := &postgresIntegrationPublisher{}

	processed, err = ProcessOne(
		ctx,
		processorConn,
		retryPublisher,
		"wu05-process-one-retry",
		time.Now().UTC().Add(5*time.Minute),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("ProcessOne retry path: %v", err)
	}

	if !processed {
		t.Fatal("ProcessOne retry path processed = false, want true")
	}

	if len(retryPublisher.records) != 1 {
		t.Fatalf(
			"retry publisher records = %d, want 1",
			len(retryPublisher.records),
		)
	}

	retryRecord := retryPublisher.records[0]

	if retryRecord.EventID != processorFailureEvent.ID {
		t.Fatalf(
			"retry changed event id: got %q want %q",
			retryRecord.EventID,
			processorFailureEvent.ID,
		)
	}

	if retryRecord.EventSource != processorFailureEvent.Source {
		t.Fatalf(
			"retry changed event source: got %q want %q",
			retryRecord.EventSource,
			processorFailureEvent.Source,
		)
	}

	assertJSONSemanticallyEqual(
		t,
		retryRecord.Envelope,
		processorFailureEvent.Envelope,
	)

	if retryRecord.AttemptCount != 2 {
		t.Fatalf(
			"retry attempt count = %d, want 2",
			retryRecord.AttemptCount,
		)
	}

	var (
		retryState     string
		retryAttempts  int
		retryOwner     *string
		retryPublished *time.Time
	)

	if err := pool.QueryRow(
		ctx,
		`SELECT state, attempt_count, lease_owner, published_at
   FROM `+schemaIdentifier+`.atlazora_outbox
  WHERE event_source = $1
    AND event_id = $2`,
		processorFailureEvent.Source,
		processorFailureEvent.ID,
	).Scan(
		&retryState,
		&retryAttempts,
		&retryOwner,
		&retryPublished,
	); err != nil {
		t.Fatalf("read ProcessOne retry final state: %v", err)
	}

	if retryState != "published" {
		t.Fatalf(
			"ProcessOne retry final state = %q, want published",
			retryState,
		)
	}

	if retryAttempts != 2 {
		t.Fatalf(
			"ProcessOne retry final attempts = %d, want 2",
			retryAttempts,
		)
	}

	if retryOwner != nil {
		t.Fatalf(
			"ProcessOne retry retained lease owner %q",
			*retryOwner,
		)
	}

	if retryPublished == nil {
		t.Fatal("ProcessOne retry published_at is nil")
	}
	// Operational-visibility proof: exercise the WU05 visibility snapshot against
	// the same isolated PostgreSQL schema. Capture the existing baseline first so
	// this regression does not depend on counts left by earlier scenarios.
	if _, err := processorConn.Exec(
		ctx,
		"SET search_path TO "+schemaIdentifier,
	); err != nil {
		t.Fatalf("set visibility integration search path: %v", err)
	}

	visibilityBaseline, err := ReadVisibilitySnapshot(ctx, processorConn)
	if err != nil {
		t.Fatalf("read visibility baseline: %v", err)
	}

	visibilityNow := time.Now().UTC().Truncate(time.Microsecond)
	visibilityOldestCreatedAt := visibilityNow.Add(-42 * time.Second)

	visibilityReadyEvent := eventA
	visibilityReadyEvent.ID = "018f47a2-7b3c-7abc-8def-5123456789ab"
	visibilityReadyEvent.Envelope = []byte(
		`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-5123456789ab","source":"atlazora://core/wu05-integration","type":"com.atlazora.wu05.integration.created.v1","time":"2026-09-03T12:00:00Z","datacontenttype":"application/json","dataschema":"https://contracts.atlazora.test/events/wu05-integration/v1","data":{"probe":"visibility-ready"}}`,
	)

	if err := Enqueue(ctx, processorConn, visibilityReadyEvent); err != nil {
		t.Fatalf("enqueue visibility ready event: %v", err)
	}

	if _, err := processorConn.Exec(
		ctx,
		`UPDATE atlazora_outbox
SET created_at = $1,
    available_at = $2
WHERE event_source = $3
  AND event_id = $4`,
		visibilityOldestCreatedAt,
		visibilityNow.Add(-time.Second),
		visibilityReadyEvent.Source,
		visibilityReadyEvent.ID,
	); err != nil {
		t.Fatalf("prepare visibility ready event: %v", err)
	}

	visibilityFailedEvent := eventA
	visibilityFailedEvent.ID = "018f47a2-7b3c-7abc-8def-6123456789ab"
	visibilityFailedEvent.Envelope = []byte(
		`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-6123456789ab","source":"atlazora://core/wu05-integration","type":"com.atlazora.wu05.integration.created.v1","time":"2026-09-03T12:00:00Z","datacontenttype":"application/json","dataschema":"https://contracts.atlazora.test/events/wu05-integration/v1","data":{"probe":"visibility-failed-retry"}}`,
	)

	if err := Enqueue(ctx, processorConn, visibilityFailedEvent); err != nil {
		t.Fatalf("enqueue visibility failed event: %v", err)
	}

	if _, err := processorConn.Exec(
		ctx,
		`UPDATE atlazora_outbox
SET attempt_count = 3,
    available_at = $1,
    last_error = $2
WHERE event_source = $3
  AND event_id = $4`,
		visibilityNow.Add(-time.Second),
		"visibility integration failure",
		visibilityFailedEvent.Source,
		visibilityFailedEvent.ID,
	); err != nil {
		t.Fatalf("prepare visibility failed event: %v", err)
	}

	recorded, err := RecordConsumption(
		ctx,
		processorConn,
		"wu05.integration.visibility-consumer",
		visibilityReadyEvent.Source,
		visibilityReadyEvent.ID,
	)
	if err != nil {
		t.Fatalf("record visibility consumption marker: %v", err)
	}

	if !recorded {
		t.Fatal("visibility consumption marker recorded = false, want true")
	}

	visibilitySnapshot, err := ReadVisibilitySnapshot(ctx, processorConn)
	if err != nil {
		t.Fatalf("read visibility snapshot: %v", err)
	}

	if visibilitySnapshot.ReadyBacklogCount != visibilityBaseline.ReadyBacklogCount+2 {
		t.Fatalf(
			"visibility ready backlog = %d, want baseline %d + 2",
			visibilitySnapshot.ReadyBacklogCount,
			visibilityBaseline.ReadyBacklogCount,
		)
	}

	if visibilitySnapshot.ProcessingCount != visibilityBaseline.ProcessingCount {
		t.Fatalf(
			"visibility processing count = %d, want baseline %d",
			visibilitySnapshot.ProcessingCount,
			visibilityBaseline.ProcessingCount,
		)
	}

	if visibilitySnapshot.FailedPendingCount != visibilityBaseline.FailedPendingCount+1 {
		t.Fatalf(
			"visibility failed pending = %d, want baseline %d + 1",
			visibilitySnapshot.FailedPendingCount,
			visibilityBaseline.FailedPendingCount,
		)
	}

	if visibilitySnapshot.RetriedEventCount != visibilityBaseline.RetriedEventCount+1 {
		t.Fatalf(
			"visibility retried events = %d, want baseline %d + 1",
			visibilitySnapshot.RetriedEventCount,
			visibilityBaseline.RetriedEventCount,
		)
	}

	if visibilitySnapshot.ConsumptionMarkerCount != visibilityBaseline.ConsumptionMarkerCount+1 {
		t.Fatalf(
			"visibility consumption markers = %d, want baseline %d + 1",
			visibilitySnapshot.ConsumptionMarkerCount,
			visibilityBaseline.ConsumptionMarkerCount,
		)
	}

	if visibilitySnapshot.OldestReadyCreatedAt == nil {
		t.Fatal("visibility oldest ready created_at is nil")
	}

	if visibilitySnapshot.OldestReadyCreatedAt.After(visibilityOldestCreatedAt) {
		t.Fatalf(
			"visibility oldest ready created_at = %s, want <= %s",
			visibilitySnapshot.OldestReadyCreatedAt.Format(time.RFC3339Nano),
			visibilityOldestCreatedAt.Format(time.RFC3339Nano),
		)
	}

	if lag := visibilitySnapshot.OldestReadyLag(visibilityNow); lag < 42*time.Second {
		t.Fatalf(
			"visibility oldest ready lag = %v, want >= 42s",
			lag,
		)
	}
}
