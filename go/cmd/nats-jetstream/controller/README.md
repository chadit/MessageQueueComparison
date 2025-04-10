# NATS JetStream Controller

## Overview
The NATS JetStream Controller is the central hub for managing the message queue. It receives events from the client, processes them, and routes them to the appropriate consumers. The controller also interacts with the database for storing and retrieving event data.

## Code Flow

### Receiving Events
The controller receives events from the client via gRPC. The `routes.go` file defines the gRPC endpoints, and the `queue.go` file handles the logic for processing and routing events.

#### Code Example
```go
// routes.go
func (s *Server) NewEvent(ctx context.Context, req *pb.NewEventRequest) (*pb.NewEventResponse, error) {
    event := &Event{
        Type: req.GetEventType(),
        Payload: req.GetPayload(),
    }

    correlationID, err := s.queue.Publish(event)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to publish event: %v", err)
    }

    return &pb.NewEventResponse{
        Ack: true,
        Event: &pb.Event{
            CorrelationId: correlationID,
        },
    }, nil
}
```

### Publishing to the Queue
The `queue.go` file contains the logic for publishing events to the NATS JetStream queue. It ensures that events are properly formatted and sent to the correct stream.

#### Code Example
```go
// queue.go
func (q *Queue) Publish(event *Event) (string, error) {
    correlationID := generateCorrelationID()
    msg := &nats.Msg{
        Subject: event.Type,
        Data:    event.Payload,
        Header:  nats.Header{"Correlation-ID": []string{correlationID}},
    }

    if err := q.conn.PublishMsg(msg); err != nil {
        return "", err
    }

    return correlationID, nil
}
```

### Database Interaction
The controller interacts with the database to store and retrieve event data. The `db.go` file contains the logic for database operations.

#### Code Example
```go
// db.go
func (db *Database) SaveEvent(event *Event) error {
    query := "INSERT INTO events (id, type, payload) VALUES (?, ?, ?)"
    _, err := db.conn.Exec(query, event.ID, event.Type, event.Payload)
    return err
}
```

## Checking Event Status
The controller provides a gRPC route to check the status of an event. Clients can call this route with a correlation ID to retrieve the current status of the event.

#### Code Example
```go
// routes.go
func (s *Server) EventStatus(ctx context.Context, req *pb.EventStatusRequest) (*pb.EventStatusResponse, error) {
    status, err := s.queue.GetStatus(req.GetCorrelationId())
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to get event status: %v", err)
    }

    return &pb.EventStatusResponse{
        Status: status,
    }, nil
}
```

## High-Level Flow
1. The client sends an event to the controller via gRPC.
2. The controller processes the event and publishes it to the NATS JetStream queue.
3. The controller stores the event data in the database.
4. Consumers subscribe to the queue and process the events as they arrive.
5. Clients can check the status of an event by calling the gRPC route with the correlation ID.

## Docker Compose
The `go-nats-jetstream-compose.yml` file includes the configuration for the controller. It sets up the necessary services and dependencies for running the controller in a Dockerized environment.