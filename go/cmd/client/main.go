package main

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/logger"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

// eventTypes are the types of events that nodes on the message bus can process.
var (
	eventTypes       = []string{"create", "deploy", "clone", "delete", "list"}
	requestProcess   = sync.Map{}
	simulateFailure  = []string{}
	mu               sync.Mutex
	throttleInterval = 100 * time.Millisecond // 30 * time.Second //
)

type Client struct {
	logger *slog.Logger
}

// New creates a new consumer with the given configuration.
func New(logLevel slog.Leveler, logOutput io.Writer) *Client {
	return &Client{
		logger: logger.New(logLevel, logOutput),
	}
}

// produceEvent sends a new event to the message service.
// simulates continuous event activity.
func (c *Client) produceEvent(ctx context.Context, client pb.MessagingServiceClient) {
	go func() {
		var loopToFalureCount int32

		for {
			select {
			case <-ctx.Done():
				c.logger.InfoContext(ctx, "client shutting down")

				return
			default:
				atomic.AddInt32(&loopToFalureCount, 1)

				// Pick a random event type.
				//nolint:gosec // this is not a security issue.
				eventType := eventTypes[rand.Intn(len(eventTypes))]

				var failEvent bool

				if loopToFalureCount%10 == 0 {
					failEvent = true
				}

				// send message to grpc service, verify the ack response.
				resp, err := client.NewEvent(ctx, &pb.NewEventRequest{
					EventType:       eventType,
					Payload:         []byte("Event " + eventType + " at " + time.Now().Format(time.RFC3339)),
					SimulateFailure: failEvent,
				})
				if err != nil {
					c.logger.ErrorContext(ctx, "error sending new event", "error", err)

					continue
				}

				if !resp.GetAck() {
					c.logger.ErrorContext(ctx, "failed to receive ack")

					continue
				}

				correlationID := resp.GetEvent().GetCorrelationId()
				if correlationID == "" {
					c.logger.ErrorContext(ctx, "failed to get correlation id")

					continue
				}

				mu.Lock()
				simulateFailure = append(simulateFailure, correlationID)
				mu.Unlock()

				start := time.Now()

				// store the reqeust start and correlation id that is created by the server.
				requestProcess.Store(correlationID, start)

				c.logger.InfoContext(ctx, "event was queued by controller", "event_type", eventType, "correlation_id", correlationID)

				time.Sleep(throttleInterval)
			}
		}
	}()
}

// checkStatus checks the status of the event.
func (c *Client) checkStatus(ctx context.Context, client pb.MessagingServiceClient) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				c.logger.InfoContext(ctx, "client shutting down")

				return
			default:
				var processCount int32

				requestProcess.Range(func(key, value any) bool {
					atomic.AddInt32(&processCount, 1)

					correlationID, isValid := key.(string)
					if !isValid {
						c.logger.ErrorContext(ctx, "check status error getting correlation id")

						return true
					}

					resp, err := client.EventStatus(ctx, &pb.EventStatusRequest{
						CorrelationId: correlationID,
					})
					if err != nil {
						c.logger.ErrorContext(ctx, "check status error getting event status", "error", err)

						return true
					}

					// if the event is not finished or failed, return and check the next event.
					switch resp.GetStatus() {
					case pb.EventStatus_EVENT_STATUS_FAILED:
						// c.logger.ErrorContext(ctx, "event failed", "correlation_id", correlationID)
					case pb.EventStatus_EVENT_STATUS_FINISHED:
					case pb.EventStatus_EVENT_STATUS_STARTED:
						c.logger.InfoContext(ctx, "check status event started", "correlation_id", correlationID)

						return true
					case pb.EventStatus_EVENT_STATUS_PENDING, pb.EventStatus_EVENT_STATUS_UNSPECIFIED:
						// c.logger.InfoContext(ctx, "event pending", "correlation_id", correlationID)
						return true
					default:
						c.logger.ErrorContext(ctx, "check status error getting event status", "status", resp.GetStatus().String())

						return true
					}

					start, isValidTime := value.(time.Time)
					if !isValidTime {
						c.logger.ErrorContext(ctx, "check status error getting start time")

						return true
					}

					mu.Lock()
					for i, v := range simulateFailure {
						if v == correlationID {
							simulateFailure = slices.Delete(simulateFailure, i, i+1)

							c.logger.InfoContext(ctx, "check status --------------------------- simulated event------------------", "correlation_id", correlationID, "duration", time.Since(start).Milliseconds())
						}
					}

					mu.Unlock()

					c.logger.InfoContext(ctx, "check status event completed", "status", resp.GetStatus().String(), "correlation_id", correlationID, "duration", time.Since(start).Milliseconds())

					// delete from requestProcess map if the event is finished.
					requestProcess.Delete(correlationID)

					return true
				})

				c.logger.InfoContext(ctx, "process count", "count", processCount)
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

func main() {
	logLevel := slog.LevelDebug
	logOutput := os.Stdout

	srv := New(logLevel, logOutput)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv.logger.InfoContext(ctx, "Starting client")

	serverAddr := config.GetGRPCServerAddress("controller")

	// Connect to the gRPC server.
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.logger.ErrorContext(ctx, "did not connect", "error", err)

		return
	}

	defer conn.Close()
	client := pb.NewMessagingServiceClient(conn)

	// randomly add an event to the GRPC server every x.
	srv.produceEvent(ctx, client)

	// check the status of the event.
	srv.checkStatus(ctx, client)

	// wait for the context to be done.
	<-ctx.Done()
}
