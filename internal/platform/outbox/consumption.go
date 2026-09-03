package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/atlazora/atlazora-core/internal/platform/database"
)

var (
	ErrConsumerNameRequired = errors.New("consumer name is required")
)

// RecordConsumption atomically records that a logical consumer has accepted
// a CloudEvent identity.
//
// The approved duplicate boundary is:
//
// consumer_name + event_source + event_id
//
// A true result means this transaction inserted the consumption marker and
// therefore owns first processing of that identity. A false result means the
// same logical consumer has already recorded the same source + id.
//
// Callers that mutate authoritative business state must call this function
// using the same database transaction as that mutation so the business write
// and idempotency marker commit or roll back together.
func RecordConsumption(
	ctx context.Context,
	db database.DBTX,
	consumerName string,
	eventSource string,
	eventID string,
) (bool, error) {
	if !dbRequired(db) {
		return false, ErrDatabaseRequired
	}

	if strings.TrimSpace(consumerName) == "" {
		return false, ErrConsumerNameRequired
	}

	if strings.TrimSpace(eventSource) == "" {
		return false, ErrEventSourceRequired
	}

	if strings.TrimSpace(eventID) == "" {
		return false, ErrEventIDRequired
	}

	const statement = `
INSERT INTO atlazora_event_consumption (
consumer_name,
event_source,
event_id
)
VALUES ($1, $2, $3)
ON CONFLICT (consumer_name, event_source, event_id)
DO NOTHING
`

	tag, err := db.Exec(
		ctx,
		statement,
		consumerName,
		eventSource,
		eventID,
	)
	if err != nil {
		return false, fmt.Errorf("record event consumption: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}
