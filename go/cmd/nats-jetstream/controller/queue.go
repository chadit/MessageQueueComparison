package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/natsqueue"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

var (
	errTimeOutWaitingForMessageQueue = errors.New("timeout waiting for message queue to be ready")
	startupBackoffInterval           = 2 * time.Second
)

type PartitionMetadataOffset struct {
	ID          int32
	StartOffset int64
	EndOffset   int64
}

// waitForMessageQueue waits for the message queue to be ready.
func (c *Controller) waitForMessageQueue(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	isSync := true
	isNoConsumer := true

	healthcheckQueue, err := natsqueue.NewConsumer(
		ctx,
		natsqueue.WithConnection[string](c.nats),
		natsqueue.WithLogger[string](c.logger),
		natsqueue.WithStreamOptions[string]("admin_stream", []string{}, isSync, isNoConsumer),
	)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}

	for time.Now().Before(deadline) {
		err := healthcheckQueue.Ping(ctx, timeout)
		if err == nil {
			return nil
		}

		c.logger.DebugContext(ctx, "waiting for message queue", "time_remaining", time.Until(time.Now()), "error", err)
		time.Sleep(startupBackoffInterval)
	}

	return errTimeOutWaitingForMessageQueue
}

// startMessageServiceHealthCheck launches a goroutine that pings every minute.
func (c *Controller) startMessageServiceHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		// stop the ticker when the function returns.
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				c.logger.InfoContext(ctx, "message service health check shutting down")

				return
			case <-ticker.C:
				if err := c.waitForMessageQueue(ctx, config.DefaultTimeoutHealthCheck); err != nil {
					c.logger.ErrorContext(ctx, "message service health check failed", "error", err)

					continue
				}

				c.logger.DebugContext(ctx, "message service health check successful")
			}
		}
	}()
}

// startMessageServiceHealthCheck launches a goroutine that pings every minute.
func (c *Controller) startQueueReport(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Minute)

	isSync := true
	isNoConsumer := true

	healthcheckQueue, err := natsqueue.NewConsumer(
		ctx,
		natsqueue.WithConnection[string](c.nats),
		natsqueue.WithLogger[string](c.logger),
		natsqueue.WithStreamOptions[string]("admin_stream", []string{}, isSync, isNoConsumer),
	)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}

	go func() {
		// stop the ticker when the function returns.
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				c.logger.InfoContext(ctx, "queue report shutting down")

				return
			case <-ticker.C:
				c.logger.DebugContext(ctx, "queue report is running")

				accountInfo, err := healthcheckQueue.GetAccountInfo(ctx)
				if err != nil {
					c.logger.ErrorContext(ctx, "failed to get account info", "error", err)

					continue
				}

				c.logger.InfoContext(ctx, "account info", "account_info", accountInfo)
			}
		}
	}()

	return nil
}

// listenForStatusUpdate listens for status updates from the message service.
func (c *Controller) listenForStatusUpdate(ctx context.Context) error {
	go func() {
		// Nats does not support . in stream names.
		// config has . because other message brokers support it.
		// ex. status_processing_stream
		// stream name is used to create a parent stream that child consumers can use.
		// subject uses a wildcard to match all events for this stream group.
		// ex. status.*.*
		streamGroupID := strings.ReplaceAll(config.DefaultStatusGroupID, ".", "_")
		streamName := streamGroupID + "_stream"
		streamSubject := config.DefaultStatusTopic + ".>"

		// since there is only one controller, the consumer name and subjects are the same.
		// if this was split into regions, something like the namespace could be used to fine turn the status messages this controller listens for.

		consumerName := streamGroupID + "_consumer"
		consumerSubjects := config.DefaultStatusTopic + ".>"

		isSync := true
		isNoConsumer := false

		statusQueue, err := natsqueue.NewConsumer(
			ctx,
			natsqueue.WithConnection[pb.EventStatusResponse](c.nats),
			natsqueue.WithStreamOptions[pb.EventStatusResponse](streamName, []string{streamSubject}, isSync, isNoConsumer),
			natsqueue.WithConsumerOptions[pb.EventStatusResponse](consumerName, []string{consumerSubjects}),
			natsqueue.WithLogger[pb.EventStatusResponse](c.logger),
			natsqueue.WithUnmarshalFunc(func(data []byte, v *pb.EventStatusResponse) error {
				if err := proto.Unmarshal(data, v); err != nil {
					//nolint:wrapcheck
					return err
				}

				return nil
			}),
		)
		if err != nil {
			c.logger.ErrorContext(ctx, "failed to create status consumer", "error", err)

			return
		}

		for {
			select {
			case <-ctx.Done():
				c.logger.InfoContext(ctx, "status listener is shutting down")

				return
			default:
				// pull the first status update message from the queue.
				result, queueRecord, err := statusQueue.PullFirstAndUnmarshal(ctx, config.DefaultTimeoutReadMessage)
				if err != nil {
					if errors.Is(err, natsqueue.ErrClientClosed) || errors.Is(err, natsqueue.ErrNoMessageFound) {
						continue
					}

					c.logger.ErrorContext(ctx, "error reading status message", "error", err)

					continue
				}

				correlationID := result.GetEvent().GetCorrelationId()
				eventType := result.GetEvent().GetEventType()
				status := result.GetStatus()

				c.logger.InfoContext(ctx, "status update received", "correlation_id", correlationID, "status", status.String(), "status_num", int(status.Number()), "event_type", eventType)

				if err := InsertEvent(c.db, &EventTable{
					CorrelationID: correlationID,
					EventType:     eventType,
					Status:        int(status.Number()),
				}); err != nil {
					c.logger.ErrorContext(ctx, "error upserting status", "error", err)

					continue
				}

				// commit the record to mark it as processed.
				if err := statusQueue.CommitRecord(ctx, queueRecord); err != nil {
					c.logger.ErrorContext(ctx, "error committing record", "error", err, "correlation_id", correlationID)
				}
			}
		}
	}()

	return nil
}
