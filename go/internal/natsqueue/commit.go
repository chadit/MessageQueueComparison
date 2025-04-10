package natsqueue

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

// CommitMessage sets the message that is being processed as committed in the queue.
// this will allow the message to be removed from the queue and not be processed again.
// without this, the message would be reprocessed if the consumer restarts or by another consumer.
func (q *Queue[T]) CommitRecord(_ context.Context, msg jetstream.Msg) error {
	// Commit the message in the queue.
	//nolint:wrapcheck
	return msg.Ack()
}
