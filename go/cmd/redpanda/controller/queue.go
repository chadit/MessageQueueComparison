package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/kafkaqueue"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

var (
	errTimeOutWaitingForMessageQueue = errors.New("timeout waiting for message queue to be ready")
	errAllNodesNotReady              = errors.New("all nodes are not ready")
	startupBackoffInterval           = 2 * time.Second
)

type PartitionMetadataOffset struct {
	ID          int32
	StartOffset int64
	EndOffset   int64
}

// pingMessageService tries to open a connection and fetch partitions as a health check.
func (c *Controller) healthCheckQueueService(ctx context.Context) error {
	// create a new client with the provided brokers.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.brokers...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	defer client.Close()

	if err = client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping client: %w", err)
	}

	// Create an admin client using the kgo client.
	admin := kadm.NewClient(client)

	// List brokers to verify the cluster is healthy.
	brokersInfo, err := admin.ListBrokers(ctx)
	if err != nil {
		return fmt.Errorf("failed to describe cluster: %w", err)
	}

	nodeCount := len(brokersInfo.NodeIDs())

	if nodeCount == 0 || nodeCount != len(c.brokers) {
		return errAllNodesNotReady
	}

	_, err = admin.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	return nil
}

// waitForMessageQueue waits for the message queue to be ready.
func (c *Controller) waitForMessageQueue(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := c.healthCheckQueueService(ctx)
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
				if err := c.healthCheckQueueService(ctx); err != nil {
					c.logger.ErrorContext(ctx, "message service health check failed", "error", err)

					continue
				}

				c.logger.DebugContext(ctx, "message service health check successful")
			}
		}
	}()
}

// startMessageServiceHealthCheck launches a goroutine that pings every minute.
func (c *Controller) startQueueReport(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
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

				client, err := kgo.NewClient(kgo.SeedBrokers(c.brokers...))
				if err != nil {
					c.logger.ErrorContext(ctx, "failed to create client", "error", err)

					continue
				}
				defer client.Close()

				// Create an admin client.
				admin := kadm.NewClient(client)

				// Set a timeout for the admin operation.
				ctxTopic, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()

				metadata, err := admin.Metadata(ctxTopic)
				if err != nil {
					c.logger.ErrorContext(ctx, "failed to fetch metadata", "error", err)

					continue
				}

				results := make(map[string]map[int32]PartitionMetadataOffset)

				for _, topic := range metadata.Topics {
					listedStartOffsets, err := admin.ListStartOffsets(ctx, topic.Topic)
					if err != nil {
						c.logger.ErrorContext(ctx, "failed to list start offsets", "error", err, "topic", topic.Topic)

						continue
					}

					listedEndOffsets, err := admin.ListEndOffsets(ctx, topic.Topic)
					if err != nil {
						c.logger.ErrorContext(ctx, "failed to list end offsets", "error", err, "topic", topic.Topic)

						continue
					}

					if len(listedEndOffsets) != len(listedStartOffsets) {
						c.logger.ErrorContext(ctx, "failed to list end offsets", "error", err, "topic", topic.Topic)

						continue
					}

					// result := make(map[int32]PartitionMetadataOffset, len(listedStartOffsets[topic.Topic]))

					for partition, detailsStart := range listedStartOffsets[topic.Topic] {
						detailsEnd, ok := listedEndOffsets[topic.Topic][partition]
						if !ok {
							c.logger.ErrorContext(ctx, "failed to get end offset for partition", "topic", topic.Topic, "partition", partition)

							continue
						}

						// check if topic exists in the result map.
						if _, ok := results[topic.Topic]; !ok {
							results[topic.Topic] = make(map[int32]PartitionMetadataOffset)
						}

						// add the partition details to the
						// result map.
						results[topic.Topic][partition] = PartitionMetadataOffset{
							ID:          partition,
							StartOffset: detailsStart.Offset,
							EndOffset:   detailsEnd.Offset,
						}
					}
				}

				for topic, result := range results {
					for partition, details := range result {
						c.logger.InfoContext(ctx, "partition details", "topic", topic, "partition", partition, "start_offset", details.StartOffset, "end_offset", details.EndOffset)
					}
				}
			}
		}
	}()
}

// listenForStatusUpdate listens for status updates from the message service.
func (c *Controller) listenForStatusUpdate(ctx context.Context) error {
	go func() {
		queue, err := kafkaqueue.NewQueue(
			c.brokers,
			config.DefaultStatusGroupID,
			config.DefaultStatusTopic,
			c.logger,
			func(data []byte, v *pb.EventStatusResponse) error {
				if err := proto.Unmarshal(data, v); err != nil {
					//nolint:wrapcheck
					return err
				}

				return nil
			})
		if err != nil {
			c.logger.ErrorContext(ctx, "failed to create status client", "error", err)

			return
		}

		defer queue.Close()

		for {
			select {
			case <-ctx.Done():
				c.logger.InfoContext(ctx, "status listener is shutting down")

				return
			default:
				c.logger.DebugContext(ctx, "status listener checking for new status update")
				// pull the first status update message from the queue.
				result, queueRecord, err := queue.PullFirstAndUnmarshal(ctx, config.DefaultTimeoutReadMessage)
				if err != nil {
					if errors.Is(err, kafkaqueue.ErrClientClosed) || errors.Is(err, kafkaqueue.ErrNoMessageFound) {
						c.logger.DebugContext(ctx,
							"no new status message found",
							"group_id", config.DefaultStatusGroupID,
							"topic", config.DefaultStatusTopic,
							"is_closed", errors.Is(err, kafkaqueue.ErrClientClosed),
							"is_no_message", errors.Is(err, kafkaqueue.ErrNoMessageFound),
						)

						continue
					}

					// in this example, if messages are processed faster then they are produced, this will show a context timeout error.
					// this is expected for this example, it is safe to ignore in this case.
					if err.Error() == "context deadline exceeded" {
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
				if err := queue.CommitRecord(ctx, queueRecord); err != nil {
					c.logger.ErrorContext(ctx, "error committing record", "error", err, "correlation_id", correlationID)
				}
			}
		}
	}()

	return nil
}
