package lifecycle

import (
	"context"
	"testing"
	"time"
)

func TestSignalContextCanBeCanceled(t *testing.T) {
	ctx, cancel := SignalContext(context.Background())
	cancel()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context was not canceled")
	}
}
