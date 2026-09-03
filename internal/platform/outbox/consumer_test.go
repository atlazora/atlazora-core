package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type recordingValidator struct {
	calls    int
	envelope json.RawMessage
	err      error
	order    *[]string
}

func (v *recordingValidator) Validate(_ context.Context, envelope json.RawMessage) error {
	v.calls++
	v.envelope = append(json.RawMessage(nil), envelope...)

	if v.order != nil {
		*v.order = append(*v.order, "validate")
	}

	return v.err
}

type recordingHandler struct {
	calls    int
	envelope json.RawMessage
	err      error
	order    *[]string
}

func (h *recordingHandler) Handle(_ context.Context, envelope json.RawMessage) error {
	h.calls++
	h.envelope = append(json.RawMessage(nil), envelope...)

	if h.order != nil {
		*h.order = append(*h.order, "handle")
	}

	return h.err
}

func validConsumedEnvelope() json.RawMessage {
	return json.RawMessage(
		`{"specversion":"1.0","id":"018f47a2-7b3c-7abc-8def-0123456789ab","source":"atlazora://core/test","type":"com.atlazora.test.created.v1","time":"2026-09-03T12:00:00Z","datacontenttype":"application/json","dataschema":"https://contracts.atlazora.test/events/test/v1","data":{"probe":"consumer"}}`,
	)
}

func TestProcessConsumedEventValidatesBeforeHandler(t *testing.T) {
	t.Parallel()

	order := []string{}

	validator := &recordingValidator{
		order: &order,
	}

	handler := &recordingHandler{
		order: &order,
	}

	envelope := validConsumedEnvelope()

	if err := ProcessConsumedEvent(
		context.Background(),
		envelope,
		validator,
		handler,
	); err != nil {
		t.Fatalf("ProcessConsumedEvent returned error: %v", err)
	}

	if validator.calls != 1 {
		t.Fatalf("validator calls = %d, want 1", validator.calls)
	}

	if handler.calls != 1 {
		t.Fatalf("handler calls = %d, want 1", handler.calls)
	}

	if len(order) != 2 || order[0] != "validate" || order[1] != "handle" {
		t.Fatalf("processing order = %#v, want [validate handle]", order)
	}

	if string(validator.envelope) != string(envelope) {
		t.Fatal("validator did not receive original structured envelope")
	}

	if string(handler.envelope) != string(envelope) {
		t.Fatal("handler did not receive validated structured envelope")
	}
}

func TestProcessConsumedEventStopsBeforeHandlerOnValidationFailure(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("contract rejected event")

	validator := &recordingValidator{
		err: sentinel,
	}

	handler := &recordingHandler{}

	err := ProcessConsumedEvent(
		context.Background(),
		validConsumedEnvelope(),
		validator,
		handler,
	)

	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want wrapped validation failure", err)
	}

	if validator.calls != 1 {
		t.Fatalf("validator calls = %d, want 1", validator.calls)
	}

	if handler.calls != 0 {
		t.Fatalf("handler calls = %d, want 0", handler.calls)
	}
}

func TestProcessConsumedEventSurfacesHandlerFailure(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("business handler failed")

	validator := &recordingValidator{}
	handler := &recordingHandler{
		err: sentinel,
	}

	err := ProcessConsumedEvent(
		context.Background(),
		validConsumedEnvelope(),
		validator,
		handler,
	)

	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want wrapped handler failure", err)
	}

	if validator.calls != 1 {
		t.Fatalf("validator calls = %d, want 1", validator.calls)
	}

	if handler.calls != 1 {
		t.Fatalf("handler calls = %d, want 1", handler.calls)
	}
}

func TestProcessConsumedEventRejectsInvalidJSONBeforeValidator(t *testing.T) {
	t.Parallel()

	validator := &recordingValidator{}
	handler := &recordingHandler{}

	err := ProcessConsumedEvent(
		context.Background(),
		json.RawMessage(`{"specversion":`),
		validator,
		handler,
	)

	if !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("error = %v, want %v", err, ErrEnvelopeInvalid)
	}

	if validator.calls != 0 {
		t.Fatalf("validator calls = %d, want 0", validator.calls)
	}

	if handler.calls != 0 {
		t.Fatalf("handler calls = %d, want 0", handler.calls)
	}
}

func TestProcessConsumedEventRejectsMissingBoundaries(t *testing.T) {
	t.Parallel()

	var typedNilValidator *recordingValidator
	var typedNilHandler *recordingHandler

	tests := []struct {
		name      string
		envelope  json.RawMessage
		validator Validator
		handler   Handler
		want      error
	}{
		{
			name:      "empty envelope",
			envelope:  nil,
			validator: &recordingValidator{},
			handler:   &recordingHandler{},
			want:      ErrEnvelopeRequired,
		},
		{
			name:      "nil validator",
			envelope:  validConsumedEnvelope(),
			validator: nil,
			handler:   &recordingHandler{},
			want:      ErrValidatorRequired,
		},
		{
			name:      "typed nil validator",
			envelope:  validConsumedEnvelope(),
			validator: typedNilValidator,
			handler:   &recordingHandler{},
			want:      ErrValidatorRequired,
		},
		{
			name:      "nil handler",
			envelope:  validConsumedEnvelope(),
			validator: &recordingValidator{},
			handler:   nil,
			want:      ErrHandlerRequired,
		},
		{
			name:      "typed nil handler",
			envelope:  validConsumedEnvelope(),
			validator: &recordingValidator{},
			handler:   typedNilHandler,
			want:      ErrHandlerRequired,
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			err := ProcessConsumedEvent(
				context.Background(),
				test.envelope,
				test.validator,
				test.handler,
			)

			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}
