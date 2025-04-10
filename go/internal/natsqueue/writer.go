package natsqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func (q *Queue[T]) WriteSync(ctx context.Context, subject string, payload []byte) error {
	_, err := q.js.Publish(ctx, subject, payload)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

// WriteMessage publishes a message to the NATS JetStream stream.
// It supports synchronous (blocking) or asynchronous publishing based on the q.sync flag.
func (q *Queue[T]) WriteMessage(ctx context.Context, msg *nats.Msg, timeout time.Duration) error {
	// Validate the parent context.
	if ctx.Err() != nil {
		return fmt.Errorf("WriteMessage parent context is already canceled: %w", ctx.Err())
	}

	// Synchronous publishing: block until the publish acknowledgment is received.
	if q.isSync {
		_, err := q.js.PublishMsg(ctx, msg)
		if err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}

		q.logger.Debug("message written", "subject", msg.Subject)

		return nil
	}

	// Asynchronous publishing:
	// Create a child context with timeout for the publish operation.
	produceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	future, err := q.js.PublishMsgAsync(msg)
	if err != nil {
		return fmt.Errorf("failed to write message async : %w", err)
	}

	// Wait for either the acknowledgment, an error, or a timeout.
	select {
	case pubAck := <-future.Ok():
		if pubAck == nil {
			//nolint:err113
			return errors.New("publish failed: received nil PubAck")
		}

		return nil
	case err := <-future.Err():
		return fmt.Errorf("publish failed: %w", err)
	case <-produceCtx.Done():
		return fmt.Errorf("publish timed out after %v: %w", timeout, produceCtx.Err())
	}
}
