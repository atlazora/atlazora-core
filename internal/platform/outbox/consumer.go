package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrValidatorRequired = errors.New("event validator is required")
	ErrHandlerRequired   = errors.New("event handler is required")
)

// Validator validates the complete structured CloudEvent envelope before any
// consumer handler is allowed to trust or inspect its payload.
//
// Executable event schema ownership remains in atlazora-contracts. This
// interface intentionally does not select or embed a JSON Schema engine.
type Validator interface {
	Validate(context.Context, json.RawMessage) error
}

// Handler processes an event only after Validator has accepted the complete
// structured CloudEvent envelope.
//
// Transport acknowledgement, broker delivery semantics, and idempotency
// transaction orchestration are outside this boundary.
type Handler interface {
	Handle(context.Context, json.RawMessage) error
}

// ProcessConsumedEvent enforces validation-before-handler ordering.
//
// A validation failure stops processing before the handler can inspect or trust
// event data. The original validation or handler failure remains discoverable
// through errors.Is when the implementation wraps sentinel errors.
func ProcessConsumedEvent(
	ctx context.Context,
	envelope json.RawMessage,
	validator Validator,
	handler Handler,
) error {
	if len(envelope) == 0 {
		return ErrEnvelopeRequired
	}

	if !json.Valid(envelope) {
		return ErrEnvelopeInvalid
	}

	if !consumerBoundaryRequired(validator) {
		return ErrValidatorRequired
	}

	if !consumerBoundaryRequired(handler) {
		return ErrHandlerRequired
	}

	if err := validator.Validate(ctx, envelope); err != nil {
		return fmt.Errorf("validate consumed event: %w", err)
	}

	if err := handler.Handle(ctx, envelope); err != nil {
		return fmt.Errorf("handle consumed event: %w", err)
	}

	return nil
}

func consumerBoundaryRequired(value any) bool {
	if value == nil {
		return false
	}

	reflected := reflect.ValueOf(value)

	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Ptr,
		reflect.Slice:
		return !reflected.IsNil()
	default:
		return true
	}
}
