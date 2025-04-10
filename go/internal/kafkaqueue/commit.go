package kafkaqueue

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// CommitMessage sets the message that is being processed as committed in the queue.
// this will allow the message to be removed from the queue and not be processed again.
// without this, the message would be reprocessed if the consumer restarts or by another consumer.
func (q *Queue[T]) CommitRecord(ctx context.Context, msg *kgo.Record) error {
	// Commit the message in the queue.
	//nolint:wrapcheck
	return q.client.CommitRecords(ctx, msg)
}
