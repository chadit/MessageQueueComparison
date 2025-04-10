package kafkaqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrClientClosed      = errors.New("kafka client is closed")
	ErrNoMessageFound    = errors.New("no message found")
	ErrFailedToUnmarshal = errors.New("failed to unmarshal message")
)

func (q *Queue[T]) PullFirstAndUnmarshal(ctx context.Context, timeout time.Duration) (T, *kgo.Record, error) {
	// sets a zero value for the return type.
	var zero T

	// setup timeout for context.
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	maxRecordsToFetch := 1

	// poll messages from queue.
	fetches := q.client.PollRecords(pollCtx, maxRecordsToFetch)
	if fetches.IsClientClosed() {
		return zero, nil, ErrClientClosed
	}

	if fetches.Err0() != nil {
		return zero, nil, fetches.Err0()
	}

	// loop though the records and unmarshal the message.
	iter := fetches.RecordIter()
	for !iter.Done() {
		// get the next record. If the record is nil, continue to the next record.
		record := iter.Next()
		if record == nil {
			q.logger.Debug("record is nil, continuing to next record")

			continue
		}

		// TODO: returning on error, will debat if this should be the first successful one, or if it is safer to return on error.
		// Unmarshal the record value into the specified type.
		var result T
		if err := q.unmarshalFunc(record.Value, &result); err != nil {
			return zero, record, fmt.Errorf("%w: %w", ErrFailedToUnmarshal, err)
		}

		// Return the unmarshaled result.
		return result, record, nil
	}

	// return zero value and error if no message was found.
	return zero, nil, ErrNoMessageFound
}
