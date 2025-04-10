package kafkaqueue

import (
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
)

type Queue[T any] struct {
	client        *kgo.Client
	logger        *slog.Logger
	unmarshalFunc func([]byte, *T) error
	sync          bool
}

func NewQueue[T any](brokers []string, group, topic string, logger *slog.Logger, unmarshalFunc func([]byte, *T) error) (*Queue[T], error) {
	logger.Debug("new queue created", "brokers", brokers, "group", group, "topic", topic)

	client, err := newClient(brokers, group, topic)
	if err != nil {
		return nil, err
	}

	return &Queue[T]{
		client:        client,
		logger:        logger,
		unmarshalFunc: unmarshalFunc,
		sync:          false,
	}, nil
}

func (q *Queue[T]) Close() {
	if q.client == nil {
		q.client.Close()
	}
}

func newClient(brokers []string, group, topic string) (*kgo.Client, error) {
	//nolint:wrapcheck
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.FetchMinBytes(1),                               // fetch min bytes
		kgo.FetchMaxBytes(config.DefaultMaxMessageSize),    // fetch max bytes
		kgo.FetchMaxWait(config.DefaultTimeoutReadMessage), // fetch max wait
		kgo.DisableAutoCommit(),                            // Disable auto-commit.
		// kgo.AutoCommitInterval(1*time.Hour),                // auto-commit after 1 hour.
	)
}
