package outbox

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/atlazora/atlazora-core/internal/platform/database"
)

var (
	ErrPublisherRequired = errors.New("outbox publisher is required")
)

// Publisher is the transport-independent publication boundary.
//
// Implementations may later adapt a selected broker/provider, but this
// foundation deliberately has no knowledge of broker topology, topics,
// queues, routing, delivery IDs, retry algorithms, or dead-letter policy.
type Publisher interface {
	Publish(context.Context, Record) error
}

// publisherRequired rejects both a nil interface and an interface containing
// a typed-nil implementation so ProcessOne cannot panic at the transport
// boundary.
func publisherRequired(publisher Publisher) bool {
	if publisher == nil {
		return false
	}

	value := reflect.ValueOf(publisher)

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

// ProcessOne executes one transport-independent outbox publication cycle.
//
// The caller supplies absolute lease and failure-availability timestamps.
// This keeps lease duration, polling cadence, and retry/backoff policy outside
// the persistence/runtime foundation.
//
// A returned processed value of false means no currently claimable event was
// found. A returned processed value of true means an event was claimed,
// regardless of whether transport publication ultimately succeeded.
func ProcessOne(
	ctx context.Context,
	db database.DBTX,
	publisher Publisher,
	leaseOwner string,
	leaseUntil time.Time,
	failureAvailableAt time.Time,
) (bool, error) {
	if !dbRequired(db) {
		return false, ErrDatabaseRequired
	}

	if !publisherRequired(publisher) {
		return false, ErrPublisherRequired
	}

	if strings.TrimSpace(leaseOwner) == "" {
		return false, ErrLeaseOwnerRequired
	}

	if leaseUntil.IsZero() {
		return false, ErrLeaseUntilRequired
	}

	if failureAvailableAt.IsZero() {
		return false, ErrAvailableAtRequired
	}

	record, found, err := ClaimNext(
		ctx,
		db,
		leaseOwner,
		leaseUntil,
	)
	if err != nil {
		return false, fmt.Errorf("claim outbox event for publication: %w", err)
	}

	if !found {
		return false, nil
	}

	if err := publisher.Publish(ctx, record); err != nil {
		if releaseErr := ReleaseFailed(
			ctx,
			db,
			record.OutboxID,
			leaseOwner,
			failureAvailableAt,
			err,
		); releaseErr != nil {
			return true, fmt.Errorf(
				"publish outbox event failed (%v); release failed publication: %w",
				err,
				releaseErr,
			)
		}

		return true, fmt.Errorf("publish outbox event: %w", err)
	}

	if err := MarkPublished(
		ctx,
		db,
		record.OutboxID,
		leaseOwner,
	); err != nil {
		return true, fmt.Errorf("persist published outbox event: %w", err)
	}

	return true, nil
}
