package kafkaqueue_test

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/kafkaqueue"
	"github.com/chadit/StreamProcessorsComparison/go/internal/logger"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

const testTopic = "test-topic"

// TestPullSimpleSuccess tests the PullFirstAndUnmarshal function with a simple success case.
func TestPullSimpleSuccess(t *testing.T) {
	t.Parallel()

	logwritter := io.Discard // os.Stdout

	logger := logger.New(slog.LevelDebug, logwritter)

	cluster, err := kfake.NewCluster(
		kfake.WithLogger(kfake.BasicLogger(logwritter, kfake.LogLevelDebug)),
		kfake.DefaultNumPartitions(3),
		kfake.AllowAutoTopicCreation(),
	)
	require.NoError(t, err, "should create a fake Kafka cluster")

	defer cluster.Close()

	// Create a Kafka client that connects to the fake cluster.
	queue, err := kafkaqueue.NewQueue(cluster.ListenAddrs(), "test-group", testTopic, logger, func(data []byte, v *string) error {
		*v = string(data)

		return nil
	})
	require.NoError(t, err, "should create a Kafka client")

	queuePull, err := kafkaqueue.NewQueue(cluster.ListenAddrs(), "test-group", testTopic, logger, func(data []byte, v *string) error {
		*v = string(data)

		return nil
	})
	require.NoError(t, err, "should create a Kafka client")

	defer queue.Close()

	defer queuePull.Close()

	// fake Kafka topic.
	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte("valid-message"),
	}

	// write message
	err = queue.WriteMessage(t.Context(), *record, config.DefaultTimeoutToWriteMessage)
	require.NoError(t, err, "should write a message to the Kafka topic")

	// Call the function.
	result, queueRecord, err := queuePull.PullFirstAndUnmarshal(t.Context(), config.DefaultTimeoutReadMessage)

	// Assertions.
	require.NoError(t, err)
	require.Equal(t, "valid-message", result)

	assert.NoError(t, queuePull.CommitRecord(t.Context(), queueRecord), "should not return an error")
}

// TestPullProtoSuccess tests the PullFirstAndUnmarshal function with a protobuf message.
func TestPullProtoSuccess(t *testing.T) {
	t.Parallel()

	logwritter := io.Discard // os.Stdout
	logger := logger.New(slog.LevelDebug, logwritter)

	cluster, err := kfake.NewCluster(
		kfake.WithLogger(kfake.BasicLogger(logwritter, kfake.LogLevelDebug)),
		kfake.DefaultNumPartitions(3),
		kfake.AllowAutoTopicCreation(),
	)
	require.NoError(t, err, "should create a fake Kafka cluster")

	defer cluster.Close()

	// Create a Kafka client that connects to the fake cluster.
	queue, err := kafkaqueue.NewQueue(cluster.ListenAddrs(), "test-group", testTopic, logger, func(data []byte, v *pb.EventStatusResponse) error {
		if err = proto.Unmarshal(data, v); err != nil {
			//nolint:wrapcheck
			return err
		}

		return nil
	})

	require.NoError(t, err, "should create a Kafka client")

	defer queue.Close()

	correlationID := uuid.New().String()

	protoMsg := pb.EventStatusResponse{
		Status: pb.EventStatus_EVENT_STATUS_FINISHED,
		Event: &pb.Event{
			EventType:     "event-type",
			CorrelationId: correlationID,
		},
	}

	data, err := proto.Marshal(&protoMsg)
	require.NoError(t, err, "should marshal the protobuf message")

	// fake Kafka topic.
	record := &kgo.Record{
		Topic: testTopic,
		Value: data,
	}

	// write message
	err = queue.WriteMessage(t.Context(), *record, config.DefaultTimeoutToWriteMessage)
	require.NoError(t, err, "should write a message to the Kafka topic")

	// Call the function.
	result, queueRecord, err := queue.PullFirstAndUnmarshal(t.Context(), config.DefaultTimeoutReadMessage)

	// Assertions.
	require.NoError(t, err)
	assert.IsType(t, &pb.EventStatusResponse{}, &result, "should return a pb.EventStatusResponse")
	assert.Equal(t, protoMsg.GetStatus(), result.GetStatus(), "should return the same status")
	assert.Equal(t, protoMsg.GetEvent().GetEventType(), result.GetEvent().GetEventType(), "should return the same event type")
	assert.Equal(t, protoMsg.GetEvent().GetCorrelationId(), result.GetEvent().GetCorrelationId(), "should return the same correlation ID")
	assert.Equal(t, correlationID, result.GetEvent().GetCorrelationId(), "should return the same correlation ID")

	assert.NoError(t, queue.CommitRecord(t.Context(), queueRecord), "should not return an error")
}

// TestPullNoMessageFound tests the PullFirstAndUnmarshal function when no message is found.
func TestPullNoMessageFound(t *testing.T) {
	t.Parallel()

	logwritter := io.Discard // os.Stdout
	logger := logger.New(slog.LevelDebug, logwritter)

	cluster, err := kfake.NewCluster(
		kfake.WithLogger(kfake.BasicLogger(logwritter, kfake.LogLevelDebug)),
		kfake.DefaultNumPartitions(3),
		kfake.AllowAutoTopicCreation(),
	)
	require.NoError(t, err, "should create a fake Kafka cluster")

	defer cluster.Close()

	// Create a Kafka client that connects to the fake cluster.
	queue, err := kafkaqueue.NewQueue(cluster.ListenAddrs(), "test-group", testTopic, logger, func(data []byte, v *string) error {
		*v = string(data)

		return nil
	})

	require.NoError(t, err, "should create a Kafka client")

	defer queue.Close()

	// Call the function.
	result, _, err := queue.PullFirstAndUnmarshal(t.Context(), 5*time.Second)

	// Assertions.
	require.Error(t, err)
	require.ErrorIs(t, err, kafkaqueue.ErrNoMessageFound, "should return ErrNoMessageFound")
	assert.Equal(t, "", result)
}

// TestPullUnmarshalError tests the PullFirstAndUnmarhsal function when the unmarshal function returns an error.
func TestPullUnmarshalError(t *testing.T) {
	t.Parallel()

	logwritter := io.Discard // os.Stdout
	logger := logger.New(slog.LevelDebug, logwritter)

	cluster, err := kfake.NewCluster(
		kfake.WithLogger(kfake.BasicLogger(logwritter, kfake.LogLevelDebug)),
		kfake.DefaultNumPartitions(3),
		kfake.AllowAutoTopicCreation(),
	)
	require.NoError(t, err, "should create a fake Kafka cluster")

	defer cluster.Close()

	// Create a Kafka client that connects to the fake cluster.
	queue, err := kafkaqueue.NewQueue(cluster.ListenAddrs(), "test-group", testTopic, logger, func(data []byte, v *string) error {
		*v = string(data)

		//nolint:err113
		return errors.New("unmarshal error")
	})

	require.NoError(t, err, "should create a Kafka client")

	defer queue.Close()

	// fake Kafka topic.
	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte("valid-message"),
	}

	// write message
	err = queue.WriteMessage(t.Context(), *record, config.DefaultTimeoutToWriteMessage)
	require.NoError(t, err, "should write a message to the Kafka topic")

	// Call the function.
	result, _, err := queue.PullFirstAndUnmarshal(t.Context(), config.DefaultTimeoutReadMessage)

	// Assertions.
	require.Error(t, err)
	require.ErrorIs(t, err, kafkaqueue.ErrFailedToUnmarshal, "should return ErrFailedToUnmarshal")
	assert.Equal(t, "", result)
}
