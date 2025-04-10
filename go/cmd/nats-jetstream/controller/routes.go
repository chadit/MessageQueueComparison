package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/eventstatus"
	"github.com/chadit/StreamProcessorsComparison/go/internal/natsqueue"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

var (
	errFailedToAddToQueue = errors.New("failed to add message to queue")
	// simulate multiple regions for different consumers.
	regions = []string{"mia1"} // regions = []string{"mia1", "sto1"}
)

// verify that Consumer implements the MessagingServiceServer interface.
var _ pb.MessagingServiceServer = (*Controller)(nil)

// SendMessage publishes the incoming message to a message processor.
// will be removed later when examples are updated.
func (c *Controller) SendMessage(_ context.Context, _ *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	// Immediately return an acknowledgement over gRPC.
	// The actual response will be sent asynchronously via RedPanda.
	return &pb.SendMessageResponse{
		Ack:           "Message received; request has been added to the queue.",
		CorrelationId: "remove_later",
	}, nil
}

// NewEvent simulates the creation of a new event.
func (c *Controller) NewEvent(ctx context.Context, req *pb.NewEventRequest) (*pb.NewEventResponse, error) {
	correlationID := uuid.New().String()
	eventType := req.GetEventType()

	// set the correlation ID if it is not set.
	if req.GetCorrelationId() == "" {
		req.CorrelationId = correlationID
	}

	// set the event type if it is not set.
	if eventType == "" {
		return nil, status.Error(codes.InvalidArgument, "event type is required")
	}

	// Marshal the gRPC (protobuf) message to bytes.
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal new event request: %w", err)
	}

	// Nats does not support . in stream names.
	// config has . because other message brokers support it.
	// ex. event_processing_stream
	// stream name is used to create a parent stream that child consumers can use.
	// subject uses a wildcard to match all events for this stream group.
	// ex. event.>  event.us.create
	streamGroupID := strings.ReplaceAll(config.DefaultEventGroupID, ".", "_")
	streamName := streamGroupID + "_stream"
	streamSubject := config.DefaultRequestTopic + ".>"

	isSync := true
	isNoConsumer := true

	eventWriteConsumer, err := natsqueue.NewConsumer(
		ctx,
		natsqueue.WithConnection[string](c.nats),
		natsqueue.WithStreamOptions[string](streamName, []string{streamSubject}, isSync, isNoConsumer),
		natsqueue.WithLogger[string](c.logger),
	)
	if err != nil {
		c.logger.ErrorContext(ctx, "error creating new event consumer", "error", err)

		//nolint:wrapcheck
		return nil, status.Error(codes.Internal, fmt.Errorf("new event %w", errFailedToAddToQueue).Error())
	}

	// Pick a random region.
	//nolint:gosec // This is not a security-sensitive operation.
	region := regions[rand.Intn(len(regions))]
	// e.g., event.mia1.create
	topic := fmt.Sprintf("%s.%s.%s", config.DefaultRequestTopic, region, eventType)

	// Create a new message.
	msg := nats.NewMsg(topic)
	msg.Data = data

	if err := eventWriteConsumer.WriteMessage(ctx, msg, config.DefaultTimeoutToWriteMessage); err != nil {
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
