package natsqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrClientClosed      = errors.New("nats client is closed")
	ErrNoMessageFound    = errors.New("no message found")
	ErrFailedToUnmarshal = errors.New("failed to unmarshal message")
)

// PullFirstAndUnmarshal fetches a single message from the NATS JetStream pull subscription,
// unmarshals its payload into type T using the provided unmarshal function,
// acknowledges the message, and returns the result along with the original message.
func (q *Queue[T]) PullFirstAndUnmarshal(_ context.Context, timeout time.Duration) (T, jetstream.Msg, error) {
	var zero T

	// fetch one message.
	msgs, err := q.consumer.Fetch(1, jetstream.FetchMaxWait(timeout))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return zero, nil, ErrNoMessageFound
		}

		//nolint:wrapcheck
		return zero, nil, err
	}

	// check if there was an error fetching the message.
	if msgs.Error() != nil {
		//nolint:wrapcheck
		return zero, nil, msgs.Error()
	}

	// get the firt message off the channel.
	msg := <-msgs.Messages()

	// after timeout if no message is found, the channel will return nil.
	if msg == nil {
		return zero, nil, ErrNoMessageFound
	}

	// Unmarshal the message data.
	var result T
	if err := q.unmarshalFunc(msg.Data(), &result); err != nil {
		return zero, msg, fmt.Errorf("%w: %w", ErrFailedToUnmarshal, err)
	}

	return result, msg, nil
}
