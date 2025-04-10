package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/eventstatus"
	"github.com/chadit/StreamProcessorsComparison/go/internal/kafkaqueue"
	"github.com/chadit/StreamProcessorsComparison/go/internal/logger"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

var timeSimulateWorkInterval = 3 * time.Second

var errMissingHeaderKey = errors.New("header key not found")

type Consumer struct {
	brokers   []string
	topic     string
	groupID   string
	supported []string
	logger    *slog.Logger
}

// New creates a new consumer with the given configuration.
func New(logLevel slog.Leveler, logOutput io.Writer, brokers []string, topic, groupID, supported string) *Consumer {
	// Split supported event types from a comma-separated string.
	supportedEvents := strings.Split(supported, ",")
	for i, ev := range supportedEvents {
		supportedEvents[i] = strings.TrimSpace(ev)
	}

	return &Consumer{
		brokers:   brokers,
		topic:     topic,
		groupID:   groupID,
		supported: supportedEvents,
		logger:    logger.New(logLevel, logOutput),
	}
}

// contains returns true if val is in slice.
func (c *Consumer) isEventSupported(event string) bool {
	return slices.Contains(c.supported, event)
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

	msg := kgo.Record{
		Key:       []byte(correlationID),
		Value:     data,
		Topic:     config.DefaultStatusTopic,
		Timestamp: time.Now().UTC(),
	}

	// create a new client.
	queue, err := kafkaqueue.NewQueue(
		c.brokers,
		config.DefaultStatusGroupID,
		config.DefaultStatusTopic,
		c.logger,
		func(_ []byte, _ *string) error { return nil })
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	defer queue.Close()

	c.logger.DebugContext(ctx, "Sending status update", "correlation_id", correlationID, "event_type", eventType, "status", eventStatus.String())

	// Write the message to the status topic.
	if err := queue.WriteMessage(ctx, msg, config.DefaultTimeoutToWriteMessage); err != nil {
		return fmt.Errorf("failed to write status message: %w", err)
	}

	c.logger.DebugContext(ctx, "Sent status update", "correlation_id", correlationID, "event_type", eventType, "status", eventStatus.String())

	return nil
}

// getEventTypeFromHeader extract the event type from the message headers.
func getEventTypeFromHeader(msg *kgo.Record) string {
	if msg == nil {
		return ""
	}

	for _, h := range msg.Headers {
		if h.Key == "event_type" {
			return string(h.Value)
		}
	}

	return ""
}

func (c *Consumer) ListenAndProcessEvents(ctx context.Context) error {
	// create a new client.
	queueEventRequest, err := kafkaqueue.NewQueue(c.brokers, config.DefaultEventGroupID, c.topic, c.logger, func(data []byte, v *pb.NewEventRequest) error {
		if err := proto.Unmarshal(data, v); err != nil {
			//nolint:wrapcheck
			return err
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create new event client: %w", err)
	}

	go func() {
		defer queueEventRequest.Close()

		for {
			for {
				select {
				case <-ctx.Done():
					c.logger.InfoContext(ctx, "event listener is shutting down")

					return
				default:
					// pull the first event message from the queue.
					newEvent, queueNewEventMsg, err := queueEventRequest.PullFirstAndUnmarshal(ctx, config.DefaultTimeoutReadMessage)
					if err != nil {
						if errors.Is(err, kafkaqueue.ErrClientClosed) {
							c.logger.ErrorContext(ctx, "new event client closed", "error", err)

							return
						}

						if errors.Is(err, kafkaqueue.ErrNoMessageFound) {
							c.logger.DebugContext(ctx, "no new event message found", "group_id", c.groupID, "topic", c.topic)

							time.Sleep(config.DefaultErrorLoopBackoffInterval)

							continue
						}

						if err.Error() == "context deadline exceeded" {
							continue
						}

						c.logger.ErrorContext(ctx, "error reading event message", "error", err)

						var (
							eventType     string
							correlationID string
						)

						if queueNewEventMsg != nil {
							eventType = getEventTypeFromHeader(queueNewEventMsg)
							correlationID = string(queueNewEventMsg.Key)
						}

						if eventType == "" || correlationID == "" {
							c.logger.ErrorContext(ctx, "error extracting event type", "error", errMissingHeaderKey, "correlation_id", correlationID, "event_type", eventType, "error1", err)

							continue
						}

						if err := c.sendStatusUpdate(ctx, eventstatus.EventStatusFailed, correlationID, eventType); err != nil {
							c.logger.ErrorContext(ctx, "error setting status", "status", eventstatus.EventStatusFailed.String(), "error", err)
						}

						c.logger.ErrorContext(ctx, "error processing event", "correlation_id", correlationID, "event_type", eventType)

						continue
					}

					eventType := getEventTypeFromHeader(queueNewEventMsg)
					correlationID := string(queueNewEventMsg.Key)

					if eventType == "" || correlationID == "" {
						c.logger.ErrorContext(ctx, "error extracting event type", "error", errMissingHeaderKey, "correlation_id", correlationID, "event_type", eventType)

						continue
					}

					c.logger.DebugContext(ctx, "Received message", "correlation_id", correlationID, "event_type", eventType, "is_supported", c.isEventSupported(eventType))

					// If the consumer supports the event type, process it; otherwise ignore.
					if !c.isEventSupported(eventType) {
						c.logger.DebugContext(ctx, "Ignoring event for this consumer", "correlation_id", correlationID, "event_type", eventType)

						continue
					}

					c.logger.InfoContext(ctx, "processing event", "correlation_id", correlationID, "event_type", eventType)

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
					if err := queueEventRequest.CommitRecord(ctx, queueNewEventMsg); err != nil {
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
	broker := flag.String("broker", "", "Kafka broker address")
	topic := flag.String("topic", config.GetRequestTopic(), "Kafka topic to consume from")
	groupID := flag.String("group", config.GetGroupID(), "Consumer group ID")
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
		brokers = config.GetBroker(config.DefaultRedPandaDNSName)
	}

	logLevel := slog.LevelDebug
	logOutput := os.Stdout

	srv := New(logLevel, logOutput, brokers, *topic, *groupID, *supported)

	srv.logger.Debug("Starting consumer", "broker", brokers, "topic", *topic, "group", *groupID, "supported", *supported)

	ctx := context.Background()

	if err := srv.ListenAndProcessEvents(ctx); err != nil {
		srv.logger.Error("error starting event processor", "error", err)
	}

	srv.logger.Debug("Consumer started")

	<-ctx.Done()
}
