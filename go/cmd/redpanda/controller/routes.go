package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/eventstatus"
	"github.com/chadit/StreamProcessorsComparison/go/internal/kafkaqueue"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

const (
	requestTopic = "benchmark"
	eventTopic   = "event"
)

var (
	errFailedToAddToQueue = errors.New("failed to add message to queue")
	// simulate multiple regions for different consumers.
	regions = []string{"mia1"} // regions = []string{"mia1", "sto1"}
)

// verify that Consumer implements the MessagingServiceServer interface.
var _ pb.MessagingServiceServer = (*Controller)(nil)

// SendMessage publishes the incoming message to a message processor.
func (c *Controller) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	// Generate a correlation ID.
	correlationID := uuid.New().String()
	req.CorrelationId = correlationID

	msg := kgo.Record{
		Key:       []byte(correlationID),
		Value:     []byte(req.GetContent()),
		Topic:     requestTopic,
		Timestamp: time.Now().UTC(),
	}

	// create new client.
	queue, err := kafkaqueue.NewQueue(c.brokers, "consumer-request-group", requestTopic, c.logger, func(_ []byte, _ *string) error { return nil })
	if err != nil {
		c.logger.ErrorContext(ctx, "error creating kafka client", "error", err)

		//nolint:wrapcheck
		return nil, status.Error(codes.Internal, fmt.Errorf("send message %w", errFailedToAddToQueue).Error())
	}

	defer queue.Close()

	if err := queue.WriteMessage(ctx, msg, config.DefaultTimeoutToWriteMessage); err != nil {
		c.logger.ErrorContext(ctx, "error send message queue", "error", err)

		// respond to the client with a message safe response.
		//nolint:wrapcheck
		return nil, status.Error(codes.Internal, fmt.Errorf("send message %w", errFailedToAddToQueue).Error())
	}

	c.logger.InfoContext(ctx, "send message queued", "correlation_id", correlationID)

	// Immediately return an acknowledgement over gRPC.
	// The actual response will be sent asynchronously via RedPanda.
	return &pb.SendMessageResponse{
		Ack:           "Message received; request has been added to the queue.",
		CorrelationId: correlationID,
	}, nil
}

// NewEvent simulates the creation of a new event.
func (c *Controller) NewEvent(ctx context.Context, req *pb.NewEventRequest) (*pb.NewEventResponse, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal new event request: %w", err)
	}

	// Generate a correlation ID.
	correlationID := uuid.New().String()
	eventType := req.GetEventType()

	// Pick a random region.
	//nolint:gosec // This is not a security-sensitive operation.
	region := regions[rand.Intn(len(regions))]
	topic := region + "." + eventTopic // e.g., mia1.event

	// Create a new message.

	msg := kgo.Record{
		Key:       []byte(correlationID),
		Value:     data,
		Topic:     topic,
		Timestamp: time.Now().UTC(),
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(eventType)},
		},
	}

	// create a new client.
	queue, err := kafkaqueue.NewQueue(c.brokers, config.DefaultEventGroupID, topic, c.logger, func(_ []byte, _ *string) error { return nil })
	if err != nil {
		c.logger.ErrorContext(ctx, "error creating kafka client", "error", err)

		//nolint:wrapcheck
		return nil, status.Error(codes.Internal, fmt.Errorf("new event %w", errFailedToAddToQueue).Error())
	}

	if err := queue.WriteMessage(ctx, msg, config.DefaultTimeoutToWriteMessage); err != nil {
		c.logger.ErrorContext(ctx, "error new event queue", "error", err)

		// respond to the client with a message safe response.
		//nolint:wrapcheck
		return nil, status.Error(codes.Internal, fmt.Errorf("new event %w", errFailedToAddToQueue).Error())
	}

	// update the database with the new event.
	if err := InsertEvent(c.db, &EventTable{
		CorrelationID: correlationID,
		EventType:     eventType,
		Status:        0,
	}); err != nil {
		c.logger.ErrorContext(ctx, "error inserting event into database", "error", err)
	}

	c.logger.InfoContext(ctx, "new event queued", "correlation_id", correlationID, "event_type", eventType, "topic", topic)

	// return an ack to the caller.
	return &pb.NewEventResponse{
		Ack: true,
		Event: &pb.Event{
			EventType:     eventType,
			CorrelationId: correlationID,
		},
	}, nil
}

// EventStatus returns the status of the event.
func (c *Controller) EventStatus(ctx context.Context, req *pb.EventStatusRequest) (*pb.EventStatusResponse, error) {
	// read the correlation ID from the request.
	correlationID := req.GetCorrelationId()

	// query the database for the event status.
	event, err := GetEvent(c.db, correlationID)
	if err != nil {
		c.logger.ErrorContext(ctx, "error getting event status", "error", err)

		//nolint:wrapcheck
		return nil, status.Error(codes.Internal, fmt.Errorf("event status %w", err).Error())
	}

	// create a response.
	statusGrpc, err := eventstatus.ToGrpc(eventstatus.Status(event.Status))
	if err != nil {
		c.logger.ErrorContext(ctx, "error converting event status", "error", err)

		//nolint:wrapcheck
		return nil, status.Error(codes.Internal, fmt.Errorf("event status %w", err).Error())
	}

	return &pb.EventStatusResponse{
		Status: statusGrpc,
		Event: &pb.Event{
			CorrelationId: correlationID,
			EventType:     event.EventType,
		},
	}, nil
}
