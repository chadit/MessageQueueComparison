package natsqueue_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

// const testSubject = "test-subject"

// TestPullSimpleSuccess tests a successful pull and unmarshal of a simple string message.
// func TestPullSimpleSuccess(t *testing.T) {
// 	t.Parallel()

// 	// Create a logger (using io.Discard to suppress output).
// 	logWriter := io.Discard
// 	log := logger.New(slog.LevelDebug, logWriter)
// 	// natsURL := "nats://localhost:4222" //"nats://dummy"
// 	natsURL := "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224" //"nats://dummy"
// 	marshalFunc := func(data []byte, v *string) error {
// 		*v = string(data)

// 		return nil
// 	}
// 	subject := testSubject

// 	// func NewQueue[T any](url, subject, durableName string, logger *slog.Logger, unmarshalFunc func([]byte, *T) error)
// 	// Create a new queue with a simple unmarshal function.
// 	queue, err := natsqueue.NewQueue(
// 		t.Context(),
// 		natsURL,
// 		"test-durable",
// 		subject,
// 		natsqueue.WithLogger[string](log),
// 		natsqueue.WithUnmarshalFunc(marshalFunc),
// 	)
// 	require.NoError(t, err)

// 	// 	// Create a dummy NATS message.
// 	msg := nats.NewMsg(subject + ".new")
// 	msg.Data = []byte("valid-message")

// 	// err = queue.WriteSync(t.Context(), subject+".new", msg.Data)

// 	err = queue.WriteMessage(t.Context(), msg, 20*time.Second)
// 	require.NoError(t, err)

// 	// time.Sleep(4 * time.Second)

// 	var count int32

// 	var wg sync.WaitGroup
// 	wg.Add(1)

// 	go func() {
// 		defer wg.Done()

// 		atomic.AddInt32(&count, 1)

// 		resp, _, err := queue.PullFirstAndUnmarshal(t.Context(), 2*time.Second)
// 		if err == nil {
// 			assert.Equal(t, "valid-message", resp)

// 			return
// 		}
// 	}()

// 	wg.Wait()

// 	require.LessOrEqual(t, count, int32(3), "count should be less than or equal to 3")
// }

func TestGRPCCall(t *testing.T) {
	serverAddr := "10.201.2.5:50051" // config.GetGRPCServerAddress("controller")

	// Connect to the gRPC server.
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	defer conn.Close()
	client := pb.NewMessagingServiceClient(conn)

	eventType := "list"

	resp, err := client.NewEvent(t.Context(), &pb.NewEventRequest{
		EventType:       eventType,
		Payload:         []byte("Event " + eventType + " at " + time.Now().Format(time.RFC3339)),
		SimulateFailure: false,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.GetAck())
}

// TestPullProtoSuccess tests pulling a protobuf message.
// func TestPullProtoSuccess(t *testing.T) {
// 	t.Parallel()

// 	logWriter := io.Discard
// 	lg := logger.New("debug", logWriter)

// 	queue, err := natsqueue.NewQueue[pb.EventStatusResponse]("nats://dummy", testSubject, "test-durable", lg, func(data []byte, v *pb.EventStatusResponse) error {
// 		return proto.Unmarshal(data, v)
// 	})
// 	require.NoError(t, err)

// 	correlationID := uuid.New().String()
// 	protoMsg := pb.EventStatusResponse{
// 		Status: pb.EventStatus_EVENT_STATUS_FINISHED,
// 		Event: &pb.Event{
// 			EventType:     "event-type",
// 			CorrelationId: correlationID,
// 		},
// 	}
// 	data, err := proto.Marshal(&protoMsg)
// 	require.NoError(t, err)

// 	dummyMsg := nats.NewMsg(testSubject)
// 	dummyMsg.Data = data

// 	sub := &mockSub{
// 		fetchFunc: func(num int, opts ...FetchOpt) ([]*nats.Msg, error) {
// 			return []*nats.Msg{dummyMsg}, nil
// 		},
// 	}
// 	mjs := &mockJS{
// 		pullSubFunc: func(subject, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
// 			return nil, nil
// 		},
// 	}
// 	setJS(queue, mjs)
// 	setSub(queue, sub)

// 	result, msg, err := queue.PullFirstAndUnmarshal(context.Background(), 2*time.Second)
// 	require.NoError(t, err)
// 	assert.Equal(t, protoMsg.Status, result.Status)
// 	assert.Equal(t, protoMsg.Event.EventType, result.Event.EventType)
// 	assert.Equal(t, protoMsg.Event.CorrelationId, result.Event.CorrelationId)

// 	err = commitRecord(queue, context.Background(), msg)
// 	assert.NoError(t, err)
// }

// // TestPullNoMessageFound tests when no message is available.
// func TestPullNoMessageFound(t *testing.T) {
// 	t.Parallel()

// 	logWriter := io.Discard
// 	lg := logger.New("debug", logWriter)

// 	queue, err := natsqueue.NewQueue[string]("nats://dummy", testSubject, "test-durable", lg, func(data []byte, v *string) error {
// 		*v = string(data)
// 		return nil
// 	})
// 	require.NoError(t, err)

// 	// Setup a mock subscription that returns no messages.
// 	sub := &mockSub{
// 		fetchFunc: func(num int, opts ...FetchOpt) ([]*nats.Msg, error) {
// 			return []*nats.Msg{}, nil
// 		},
// 	}
// 	mjs := &mockJS{
// 		pullSubFunc: func(subject, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
// 			return nil, nil
// 		},
// 	}
// 	setJS(queue, mjs)
// 	setSub(queue, sub)

// 	result, _, err := queue.PullFirstAndUnmarshal(context.Background(), 1*time.Second)
// 	require.Error(t, err)
// 	require.ErrorIs(t, err, natsqueue.ErrNoMessageFound)
// 	assert.Equal(t, "", result)
// }

// // TestPullUnmarshalError tests when the unmarshal function fails.
// func TestPullUnmarshalError(t *testing.T) {
// 	t.Parallel()

// 	logWriter := io.Discard
// 	lg := logger.New("debug", logWriter)

// 	queue, err := natsqueue.NewQueue[string]("nats://dummy", testSubject, "test-durable", lg, func(data []byte, v *string) error {
// 		*v = string(data)
// 		return errors.New("unmarshal error")
// 	})
// 	require.NoError(t, err)

// 	dummyMsg := nats.NewMsg(testSubject)
// 	dummyMsg.Data = []byte("invalid-message")
// 	sub := &mockSub{
// 		fetchFunc: func(num int, opts ...FetchOpt) ([]*nats.Msg, error) {
// 			return []*nats.Msg{dummyMsg}, nil
// 		},
// 	}
// 	mjs := &mockJS{
// 		pullSubFunc: func(subject, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
// 			return nil, nil
// 		},
// 	}
// 	setJS(queue, mjs)
// 	setSub(queue, sub)

// 	result, _, err := queue.PullFirstAndUnmarshal(context.Background(), 2*time.Second)
// 	require.Error(t, err)
// 	require.ErrorIs(t, err, natsqueue.ErrFailedToUnmarshal)
// 	assert.Equal(t, "", result)
// }
