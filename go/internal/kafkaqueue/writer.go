package kafkaqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrFailedToCreateWriter = errors.New("failed to create writer")
	ErrFailedToWriteMessage = errors.New("failed to write message")
)

func (q *Queue[T]) WriteMessage(ctx context.Context, msg kgo.Record, timeout time.Duration) error {
	if ctx.Err() != nil {
		return fmt.Errorf("WriteMessage parent context is already canceled: %w", ctx.Err())
	}

	if q.sync {
		results := q.client.ProduceSync(ctx, &msg)
		if results.FirstErr() != nil {
			return fmt.Errorf("%w : %w", ErrFailedToWriteMessage, results.FirstErr())
		}

		return nil
	}

	// Create a context with a timeout for this specific produce operation.
	produceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// create a channel for any errors that are received from the promise.
	errChan := make(chan error, 1)

	q.client.Produce(produceCtx, &msg, func(_ *kgo.Record, err error) {
		if err != nil {
			errChan <- fmt.Errorf("%w : %w", ErrFailedToWriteMessage, err)

			return
		}

		errChan <- nil
	})

	// Wait for the promise callback to complete or for the timeout to occur.
	select {
	case err := <-errChan:
		// Return the error from the promise callback.
		return err
	case <-produceCtx.Done():
		// Return a timeout error if the context is canceled or the deadline is exceeded.
		return fmt.Errorf("produceRecord timed out after %v: %w", timeout, produceCtx.Err())
	}
}
