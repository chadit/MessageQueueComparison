package natsqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Queue represents a generic NATS JetStream queue.
type Queue[T any] struct {
	logger *slog.Logger
	// nc       *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer

	// Stream Name.
	streamName string
	// Stream Subjects.
	streamSubjects []string

	// Consumer Name.
	consumerName string
	// Consumer Subjects.
	consumerSubjects []string

	unmarshalFunc func([]byte, *T) error
	isSync        bool // synchronous or asynchronous publishing.
	isStreamOnly  bool // true if only the jetstream stream is needed.
}

type Option[T any] func(*Queue[T])

// WithLogger sets the logger for the Queue.
func WithLogger[T any](logger *slog.Logger) Option[T] {
	return func(q *Queue[T]) {
		q.logger = logger
	}
}

// WithStreamOptions sets the stream name and subjects for the Queue.
// The stream name is used to identify the stream in JetStream.
// The subjects are used to match messages to the stream.
func WithStreamOptions[T any](name string, subjects []string, isSync, isStreamOnly bool) Option[T] {
	return func(q *Queue[T]) {
		q.streamName = name
		q.streamSubjects = subjects
		q.isStreamOnly = isStreamOnly
		q.isSync = isSync
	}
}

// WithConsumerOptions sets the consumer name and subjects for the Queue.
// The consumer name is used to identify the consumer in JetStream.
// The subjects are used to match messages to the consumer.
// The consumer name is used for pull subscription grouping.
func WithConsumerOptions[T any](name string, subjects []string) Option[T] {
	return func(q *Queue[T]) {
		q.consumerName = name
		q.consumerSubjects = subjects
	}
}

// WithUnmarshalFunc sets the unmarshal function for the Queue.
func WithUnmarshalFunc[T any](unmarshalFunc func([]byte, *T) error) Option[T] {
	return func(q *Queue[T]) {
		q.unmarshalFunc = unmarshalFunc
	}
}

func WithConnection[T any](js jetstream.JetStream) Option[T] {
	return func(q *Queue[T]) {
		q.js = js
	}
}

// NewConnection creates a new NATS JetStream context.
// 'nc' is the NATS connection.
//
//nolint:ireturn // ignore return value of jetstream.JetStream.
func NewConnection(url string) (jetstream.JetStream, error) {
	natsConn, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	jetstreamConn, err := jetstream.New(natsConn)
	if err != nil {
		// if the jetstream fails, close the connection.
		natsConn.Close()

		return nil, fmt.Errorf("failed to create jetstream context: %w", err)
	}

	return jetstreamConn, nil
}

// NewConsumer creates a new NATS JetStream queue instance.
// 'url' is the NATS server URL, 'subject' functions as a topic,
// and 'durableName' is used for pull subscription grouping.
func NewConsumer[T any](ctx context.Context, opts ...Option[T]) (*Queue[T], error) {
	var queue Queue[T]

	for _, opt := range opts {
		opt(&queue)
	}

	if queue.js == nil {
		return nil, fmt.Errorf("jetstream connetion has not been established")
	}

	// if only admin, skip stream and consumer creation.
	if queue.isStreamOnly {
		// check that the stream has been created.
		_, err := queue.getStream(ctx, queue.js)
		if err != nil {
			return nil, fmt.Errorf("failed to establish stream: %w", err)
		}

		return &queue, nil
	}

	if queue.consumer == nil {
		stream, err := queue.getStream(ctx, queue.js)
		if err != nil {
			return nil, fmt.Errorf("failed to establish stream: %w", err)
		}

		if queue.consumer, err = queue.getConsumer(ctx, queue.js, stream); err != nil {
			return nil, fmt.Errorf("failed to create consumer: %w", err)
		}
	}

	return &queue, nil
}

//nolint:ireturn // ignore return value of jetstream.Stream.
func (q *Queue[T]) getStream(ctx context.Context, parentStream jetstream.JetStream) (jetstream.Stream, error) {
	stream, err := parentStream.Stream(ctx, q.streamName)
	if err != nil {
		// return the error if it isn ot a stream not found error.
		// if the stream is not found, the stream will be created.
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("failed to get stream: %w", err)
		}
	}

	// if stream was found, return it.
	if stream != nil {
		q.logger.Debug("stream found", "name", q.streamName, "subjects", q.streamSubjects)

		return stream, nil
	}

	// Create a new stream if it does not exist.
	stream, err = parentStream.CreateStream(ctx, jetstream.StreamConfig{
		Name:     q.streamName,            // Stream name.
		Subjects: q.streamSubjects,        // Subjects to match.
		Storage:  jetstream.MemoryStorage, // use jetstream.FileStorage in prod environment.
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	q.logger.Debug("stream created", "name", q.streamName, "subjects", q.streamSubjects)

	return stream, nil
}

//nolint:ireturn // ignore return value of jetstream.Consumer.
func (q *Queue[T]) getConsumer(ctx context.Context, parentStream jetstream.JetStream, stream jetstream.Stream) (jetstream.Consumer, error) {
	// CreateOrUpdateConsumer will update the consumer if it exists, or create it if it does not exist.
	// this will also update the subjects for the consumer if they have changed.
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:           q.consumerName,                // Consumer name, must be the same as the durable name if set.
		Durable:        q.consumerName,                // durable prevents messages from being cleaned up automatically.
		AckPolicy:      jetstream.AckExplicitPolicy,   // messages wait for ack before being removed.
		DeliverPolicy:  jetstream.DeliverAllPolicy,    // deliver all messages.
		ReplayPolicy:   jetstream.ReplayInstantPolicy, // replay messages instantly.
		FilterSubjects: q.consumerSubjects,
	})
	if err != nil {
		//nolint:wrapcheck // wrap will happen at the call site.
		return nil, err
	}

	q.logger.Debug("consumer created", "name", q.consumerName, "subjects", q.consumerSubjects)

	return consumer, nil
}
