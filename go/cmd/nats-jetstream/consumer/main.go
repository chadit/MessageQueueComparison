package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/eventstatus"
	"github.com/chadit/StreamProcessorsComparison/go/internal/logger"
	"github.com/chadit/StreamProcessorsComparison/go/internal/natsqueue"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

var timeSimulateWorkInterval = 3 * time.Second

type Consumer struct {
	namespace        string
	groupID          string
	consumerSubjects []string
	logger           *slog.Logger
	nats             jetstream.JetStream
}

// New creates a new consumer with the given configuration.
func New(logLevel slog.Leveler, logOutput io.Writer, brokers []string, groupID, supportedSubjects, namespace string) (*Consumer, error) {
	// Split supported event types from a comma-separated string.
	subjects := strings.Split(supportedSubjects, ",")

	consumerSubjects := make([]string, len(subjects))

	for i, subject := range subjects {
		consumerSubjects[i] = fmt.Sprintf("%s.%s.%s", config.DefaultRequestTopic, namespace, strings.TrimSpace(subject))
	}

	// create nats jetstream.
	nats, err := natsqueue.NewConnection(strings.Join(brokers, ","))
	if err != nil {
		return nil, fmt.Errorf("failed to create nats jetstream: %w", err)
	}

	return &Consumer{
		nats:             nats,
		namespace:        namespace,
		groupID:          groupID,
		consumerSubjects: consumerSubjects,
		logger:           logger.New(logLevel, logOutput),
	}, nil
}

// Close closes the nats connection.
func (c *Consumer) Close() {
	if c.nats != nil && c.nats.Conn() != nil {
		c.nats.Conn().Close()
	}
}

// sendStatusUpdate sends an event status update to the status topic.
func (c *Consumer) sendStatusUpdate(ctx context.Context, eventStatus eventstatus.Status, correlationID, eventType string) error {
	msgStatus, err := eventstatus.ToGrpc(eventStatus)
	if err != nil {
		return fmt.Errorf("failed to convert event status: %w", err)
	}

	req := &pb.EventStatusResponse{
		Status: msgStatus,
		Event: &pb.Event{
			CorrelationId: correlationID,
			EventType:     eventType,
		},
	}

	// Marshal the gRPC (protobuf) message to bytes.
	data, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal event status response: %w", err)
	}

	// Nats does not support . in stream names.
	// config has . because other message brokers support it.
	// ex. status_processing_stream
	// stream name is used to create a parent stream that child consumers can use.
	// subject uses a wildcard to match all events for this stream group.
	// ex. status.*.*
	streamGroupID := strings.ReplaceAll(config.DefaultStatusGroupID, ".", "_")
	streamName := streamGroupID + "_stream"
	streamSubject := config.DefaultStatusTopic + ".>"

	isSync := true
	isNoConsumer := true

	statusQueue, err := natsqueue.NewConsumer(
		ctx,
		natsqueue.WithConnection[string](c.nats),
		natsqueue.WithStreamOptions[string](streamName, []string{streamSubject}, isSync, isNoConsumer),
		natsqueue.WithLogger[string](c.logger))
	if err != nil {
		return fmt.Errorf("failed to create status consumer: %w", err)
	}

	c.logger.DebugContext(ctx, "Sending status update", "correlation_id", correlationID, "event_type", eventType, "status", eventStatus.String())

	// status.event_type is use to add the subject to the message to be sent on the status topic stream.
	// ex. status.region.create
	messageSubject := fmt.Sprintf("%s.%s.%s", config.DefaultStatusTopic, c.namespace, eventType)
	msg := nats.NewMsg(messageSubject)
	msg.Data = data

	// Write the message to the status topic.
	if err := statusQueue.WriteMessage(ctx, msg, config.DefaultTimeoutToWriteMessage); err != nil {
		return fmt.Errorf("failed to write status message: %w", err)
	}

	c.logger.DebugContext(ctx, "Sent status update", "correlation_id", correlationID, "subject", messageSubject, "status", eventStatus.String())

	return nil
}

// ListenAndProcessEvents listens for new events and processes them.
//
//nolint:funlen,gocognit,cyclop
func (c *Consumer) ListenAndProcessEvents(ctx context.Context) error {
	// Nats does not support . in stream names.
	// config has . because other message brokers support it.
	// ex. event_processing_stream
	// stream name is used to create a parent stream that child consumers can use.
	// subject uses a wildcard to match all events for this stream group.
	// ex. event.>  event.us.create
	streamGroupID := strings.ReplaceAll(config.DefaultEventGroupID, ".", "_")
	streamName := streamGroupID + "_stream"
	streamSubject := config.DefaultRequestTopic + ".>"

	// consumer1_us_consumer
	consumerName := fmt.Sprintf("%s_%s_consumer", c.groupID, c.namespace)
	consumerSubjects := c.consumerSubjects

	isSync := true
	isNoConsumer := false

	// create a new client.
	natsConsumer, err := natsqueue.NewConsumer(
		ctx,
		natsqueue.WithConnection[pb.NewEventRequest](c.nats),
		natsqueue.WithStreamOptions[pb.NewEventRequest](streamName, []string{streamSubject}, isSync, isNoConsumer),
		natsqueue.WithConsumerOptions[pb.NewEventRequest](consumerName, consumerSubjects),
		natsqueue.WithLogger[pb.NewEventRequest](c.logger),
		natsqueue.WithUnmarshalFunc(
			func(data []byte, v *pb.NewEventRequest) error {
				if err := proto.Unmarshal(data, v); err != nil {
					//nolint:wrapcheck
					return err
				}

				return nil
			}))
	if err != nil {
		return fmt.Errorf("failed to create new event consumer: %w", err)
	}

	go func() {
		for {
			for {
				select {
				case <-ctx.Done():
					c.logger.InfoContext(ctx, "event listener is shutting down")

					return
				default:
					// pull the first event message from the queue.
					newEvent, queueNewEventMsg, err := natsConsumer.PullFirstAndUnmarshal(ctx, config.DefaultTimeoutReadMessage)
					if err != nil {
						if errors.Is(err, natsqueue.ErrNoMessageFound) {
							c.logger.DebugContext(ctx, "no new event message found", "group_id", c.groupID, "topics", c.consumerSubjects)

							time.Sleep(config.DefaultErrorLoopBackoffInterval)

							continue
						}

						if err.Error() == "context deadline exceeded" {
							c.logger.DebugContext(ctx, "pulling new event message timed out", "error", err)

							continue
						}

						c.logger.ErrorContext(ctx, "error processing event", "error", err)

						continue
					}

					correlationID := newEvent.GetCorrelationId()
					eventType := newEvent.GetEventType()

					messageSubject := queueNewEventMsg.Subject()

					// verify that the message subject and event type match.
					if !strings.Contains(strings.ToLower(messageSubject), strings.ToLower(eventType)) {
						c.logger.ErrorContext(ctx, "error processing event message subject and event type do not match", "subject", messageSubject, "event_type", eventType, "correlation_id", correlationID)

						if err := c.sendStatusUpdate(ctx, eventstatus.EventStatusFailed, correlationID, eventType); err != nil {
							c.logger.ErrorContext(ctx, "error setting status", "status", eventstatus.EventStatusFailed.String(), "error", err)
						}

						continue
					}

					c.logger.InfoContext(ctx, "processing event", "correlation_id", correlationID, "event_type", eventType, "subject", messageSubject)

					if config.GetIfSimulateFailure() && newEvent.GetSimulateFailure() {
						c.logger.ErrorContext(ctx, "simulating failure", "correlation_id", correlationID, "event_type", eventType)
						// Do not commit the offset; crash the process.
						os.Exit(1)
					}

					// update the status of the event.
					if err := c.sendStatusUpdate(ctx, eventstatus.EventStatusStarted, correlationID, eventType); err != nil {
						c.logger.ErrorContext(ctx, "error setting status", "status", eventstatus.EventStatusStarted.String(), "error", err)
					}

					c.logger.DebugContext(ctx, "event payload", "payload", string(newEvent.GetPayload()))

					// Simulate processing (e.g., work on the event)
					time.Sleep(timeSimulateWorkInterval)

					// update the status of the event.
					if err := c.sendStatusUpdate(ctx, eventstatus.EventStatusFinished, correlationID, eventType); err != nil {
						c.logger.ErrorContext(ctx, "error setting status", "status", eventstatus.EventStatusFinished.String(), "error", err)
					}

					// After processing successfully, commit the offset, this is to prevent reprocessing the same message.
					if err := natsConsumer.CommitRecord(ctx, queueNewEventMsg); err != nil {
						c.logger.ErrorContext(ctx, "error committing offset", "error", err)
					}

					c.logger.InfoContext(ctx, "event processed", "correlation_id", correlationID, "event_type", eventType)
				}
			}
		}
	}()

	return nil
}

func main() {
	broker := flag.String("broker", "", "queue broker address")
	groupID := flag.String("group", config.GetGroupID(), "Consumer group ID")
	namespace := flag.String("namespace", config.GetNamespace(), "Namespace for the consumer")
	supported := flag.String("supported", config.GetSupportedEvents(), "Comma-separated list of supported event types")
	flag.Parse()

	var brokers []string
	if broker != nil && *broker != "" {
		brokers = strings.Split(*broker, ",")
		for i, broker := range brokers {
			brokers[i] = strings.TrimSpace(broker)
		}
	}

	if len(brokers) == 0 {
		brokers = config.GetBroker(config.DefaultNatsDNSName)
	}

	logLevel := slog.LevelDebug
	logOutput := os.Stdout

	srv, err := New(logLevel, logOutput, brokers, *groupID, *supported, *namespace)
	if err != nil {
		srv.logger.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	defer srv.Close()

	srv.logger.Debug("Starting consumer", "broker", brokers, "group", *groupID, "supported", *supported, "namespace", *namespace)

	ctx := context.Background()

	if err := srv.ListenAndProcessEvents(ctx); err != nil {
		srv.logger.Error("error starting event processor", "error", err)
	}

	srv.logger.Debug("Consumer started")

	<-ctx.Done()
}
