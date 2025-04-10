package natsqueue

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrVersionCheckFailed = errors.New("failed to ping server")
	ErrJetStreamNotReady  = errors.New("JetStream is not ready")
)

func (q *Queue[T]) Ping(ctx context.Context, timeout time.Duration) error {
	// Send a ping message to the NATS server.
	versions := q.js.Conn().ConnectedServerVersion()
	if versions == "" {
		return ErrVersionCheckFailed
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if _, err := q.js.AccountInfo(ctx); err != nil {
		return ErrJetStreamNotReady
	}

	return nil
}

func (q *Queue[T]) GetAccountInfo(ctx context.Context) (*jetstream.AccountInfo, error) {
	//nolint:wrapcheck
	return q.js.AccountInfo(ctx)
}
