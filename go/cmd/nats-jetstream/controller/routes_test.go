package main_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	main "github.com/chadit/StreamProcessorsComparison/go/cmd/nats-jetstream/controller"
	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/logger"
	"github.com/chadit/StreamProcessorsComparison/go/internal/natsqueue"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

var eventTypes = []string{"create", "deploy", "clone", "delete", "list"}

func TestNewEvent(t *testing.T) {
	t.Setenv("NATS_BROKER", "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224")

	ctx := t.Context()
	logLevel := slog.LevelDebug
	logOutput := os.Stdout
	connectionTimeout := 30 * time.Second
	logger := logger.New(logLevel, logOutput)

	newController, err := main.NewController(ctx, logLevel, logOutput, connectionTimeout)
	require.NoError(t, err)
	require.NotNil(t, newController)

	eventType := "create"

	resp, err := newController.NewEvent(ctx, &pb.NewEventRequest{
		EventType: eventType,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)

	// setup consumer
	nats, err := natsqueue.NewConnection("nats://localhost:4222,nats://localhost:4223,nats://localhost:4224")
	require.NoError(t, err)
	require.NotNil(t, nats)

	//	defer nats.Conn().Close()

	streamGroupID := strings.ReplaceAll(config.DefaultEventGroupID, ".", "_")
	streamName := streamGroupID + "_stream"
	streamSubject := config.DefaultRequestTopic + ".>"

	groupID := "conumer1"
	namespace := "mia1"

	supportedSubjects := "create,deploy,clone,delete,list"
	subjects := strings.Split(supportedSubjects, ",")

	consumerSubjects := make([]string, len(subjects))

	for i, subject := range subjects {
		consumerSubjects[i] = fmt.Sprintf("%s.%s.%s", config.DefaultRequestTopic, namespace, strings.TrimSpace(subject))
	}

	// consumer1_us_consumer
	consumerName := fmt.Sprintf("%s_%s_consumer", groupID, namespace)

	isSync := true
	isNoConsumer := false

	readerCtx := context.Background()

	js, err := jetstream.New(nats.Conn())
	require.NoError(t, err)

	lister := js.ListStreams(ctx)
	require.NotNil(t, lister)

	for info := range lister.Info() {
		if info == nil {
			continue
		}

		fmt.Printf("Stream Name: %s\n", info.Config.Name)
		fmt.Printf("Subjects:    %v\n", info.Config.Subjects)
		fmt.Printf("Storage:     %s\n", info.Config.Storage)
		fmt.Printf("Replicas:    %d\n", info.Config.Replicas)
		fmt.Println("----")
	}

	go func() {
		for {
			select {
			case <-readerCtx.Done():
				logger.InfoContext(readerCtx, "event listener is shutting down")

				return
			default:
				natsConsumer, err := natsqueue.NewConsumer(
					readerCtx,
					natsqueue.WithConnection[pb.NewEventRequest](nats),
					natsqueue.WithStreamOptions[pb.NewEventRequest](streamName, []string{streamSubject}, isSync, isNoConsumer),
					natsqueue.WithConsumerOptions[pb.NewEventRequest](consumerName+"_a1", consumerSubjects),
					natsqueue.WithLogger[pb.NewEventRequest](logger),
					natsqueue.WithUnmarshalFunc(
						func(data []byte, v *pb.NewEventRequest) error {
							if err = proto.Unmarshal(data, v); err != nil {
								//nolint:wrapcheck
								return err
							}

							return nil
						}))
				if err != nil {
					logger.ErrorContext(readerCtx, "error creating new event consumer", "error", err)

					return
				}

				// pull the first event message from the queue.
				newEvent, queueNewEventMsg, err := natsConsumer.PullFirstAndUnmarshal(readerCtx, config.DefaultTimeoutReadMessage)
				if err != nil {
					if errors.Is(err, natsqueue.ErrNoMessageFound) {
						logger.DebugContext(ctx, "no new event message found", "group_id", groupID, "topics", consumerSubjects)

						time.Sleep(config.DefaultErrorLoopBackoffInterval)

						continue
					}

					if err.Error() == "context deadline exceeded" {
						logger.DebugContext(ctx, "pulling new event message timed out", "error", err)

						continue
					}

					logger.ErrorContext(ctx, "error processing event", "error", err)

					continue
				}

				correlationID := newEvent.GetCorrelationId()
				eventType := newEvent.GetEventType()

				messageSubject := queueNewEventMsg.Subject()

				// verify that the message subject and event type match.
				if !strings.Contains(strings.ToLower(messageSubject), strings.ToLower(eventType)) {
					logger.ErrorContext(ctx, "error processing event message subject and event type do not match", "subject", messageSubject, "event_type", eventType, "correlation_id", correlationID)

					continue
				}

				logger.InfoContext(ctx, "processing event", "correlation_id", correlationID, "event_type", eventType, "subject", messageSubject)

				if config.GetIfSimulateFailure() && newEvent.GetSimulateFailure() {
					logger.ErrorContext(ctx, "simulating failure", "correlation_id", correlationID, "event_type", eventType)
					// Do not commit the offset; crash the process.
					os.Exit(1)
				}

				logger.DebugContext(ctx, "event payload", "payload", string(newEvent.GetPayload()))

				// Simulate processing (e.g., work on the event)
				time.Sleep(5 * time.Second)

				// After processing successfully, commit the offset, this is to prevent reprocessing the same message.
				if err := natsConsumer.CommitRecord(ctx, queueNewEventMsg); err != nil {
					logger.ErrorContext(ctx, "error committing offset", "error", err)
				}

				logger.InfoContext(ctx, "event processed", "correlation_id", correlationID, "event_type", eventType)
			}
		}
	}()

	<-readerCtx.Done()
}

func TestNewMultipleEvent(t *testing.T) {
	t.Setenv("NATS_BROKER", "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224")

	logLevel := slog.LevelDebug
	logOutput := os.Stdout
	connectionTimeout := 30 * time.Second

	newController, err := main.NewController(t.Context(), logLevel, logOutput, connectionTimeout)
	require.NoError(t, err)
	require.NotNil(t, newController)

	for i := 0; i < 20; i++ {
		//nolint:gosec // this is not a security issue.
		eventType := eventTypes[rand.Intn(len(eventTypes))]

		resp, err := newController.NewEvent(t.Context(), &pb.NewEventRequest{
			EventType: eventType,
		})
		require.NoError(t, err, "failed to create event %s", eventType)
		require.NotNil(t, resp, "failed to create event %s", eventType)
		require.True(t, resp.GetAck(), "failed to create event %s", eventType)
		require.NotEmpty(t, resp.GetEvent().GetCorrelationId(), "failed to create event %s", eventType)
	}
}
