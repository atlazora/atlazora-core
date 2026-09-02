package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// SignalContext returns a context canceled when the process receives
// an operating-system termination signal.
func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(
		parent,
		os.Interrupt,
		syscall.SIGTERM,
	)
}
