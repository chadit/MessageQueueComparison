# Client Component

## Overview
The client component is responsible for producing events and checking their statuses. It interacts with the controller via gRPC to send events and retrieve their statuses. This component simulates continuous event activity and monitors the lifecycle of each event.

## Code Walkthrough

### Producing Events
The `produceEvent` function generates random events and sends them to the controller. Each event includes an event type, payload, and a flag to simulate failure. The function logs the acknowledgment and correlation ID returned by the controller.

#### Code Snippet
```go
func (c *Client) produceEvent(ctx context.Context, client pb.MessagingServiceClient) {
    go func() {
        for {
            select {
            case <-ctx.Done():
                c.logger.InfoContext(ctx, "client shutting down")
                return
            default:
                eventType := eventTypes[rand.Intn(len(eventTypes))]
                resp, err := client.NewEvent(ctx, &pb.NewEventRequest{
                    EventType: eventType,
                    Payload:   []byte("Event " + eventType + " at " + time.Now().Format(time.RFC3339)),
                })
                if (err != nil) {
                    c.logger.ErrorContext(ctx, "error sending new event", "error", err)
                    continue
                }
                c.logger.InfoContext(ctx, "event was queued by controller", "event_type", eventType, "correlation_id", resp.GetEvent().GetCorrelationId())
                time.Sleep(throttleInterval)
            }
        }
    }()
}
```

### Checking Event Status
The `checkStatus` function periodically queries the controller for the status of each event. It logs the status and duration of completed events and removes them from the in-memory map.

#### Code Snippet
```go
func (c *Client) checkStatus(ctx context.Context, client pb.MessagingServiceClient) {
    go func() {
        for {
            select {
            case <-ctx.Done():
                c.logger.InfoContext(ctx, "client shutting down")
                return
            default:
                requestProcess.Range(func(key, value any) bool {
                    correlationID := key.(string)
                    resp, err := client.EventStatus(ctx, &pb.EventStatusRequest{
                        CorrelationId: correlationID,
                    })
                    if err != nil {
                        c.logger.ErrorContext(ctx, "error getting event status", "error", err)
                        return true
                    }
                    c.logger.InfoContext(ctx, "event status", "status", resp.GetStatus().String(), "correlation_id", correlationID)
                    return true
                })
                time.Sleep(1 * time.Second)
            }
        }
    }()
}
```

## Example Calls

### Producing an Event
The client automatically produces events by calling the `produceEvent` function. For example:
```go
srv.produceEvent(ctx, client)
```

### Checking Event Status
The client periodically checks the status of events by calling the `checkStatus` function. For example:
```go
srv.checkStatus(ctx, client)
```

## High-Level Flow
1. The client connects to the gRPC server.
2. It continuously produces events and sends them to the controller.
3. It periodically checks the status of each event and logs the results.
4. The client shuts down gracefully when the context is canceled.