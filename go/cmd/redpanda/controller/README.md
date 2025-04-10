# Redpanda Controller

## Overview
The Redpanda Controller acts as the central hub for managing the Redpanda message queue. It handles incoming messages, processes them, and routes them to the appropriate consumers. It also interacts with the database for storing and retrieving event data.

## Features
- Receives events from clients via gRPC.
- Publishes events to the Redpanda message queue.
- Manages event status and database interactions.

## Usage
The Redpanda Controller is designed to work seamlessly with the Redpanda Client and Consumer. It acts as the intermediary between the client and the message queue.

## Docker Integration
The Redpanda Controller can be deployed using Docker. The provided `Dockerfile` contains the necessary instructions to build a Docker image for the controller. This allows for easy deployment and scaling in a containerized environment.

## Code Structure
- `main.go`: The entry point for the Redpanda Controller application. It initializes the controller and starts the gRPC server.
- `queue.go`: Contains the logic for interacting with the Redpanda message queue.
- `db.go`: Handles database interactions for storing and retrieving event data.
- `routes.go`: Defines the gRPC routes for the controller.

## Prerequisites
- A running instance of Redpanda.
- Properly configured topics for the controller to publish events to.

## Deployment
To deploy the Redpanda Controller, build the Docker image using the provided `Dockerfile` and run it in a containerized environment. Ensure that the Redpanda server is accessible and properly configured.

## Code Flow

The Redpanda Controller is responsible for receiving events from clients, publishing them to the message queue, and managing their status. Below is an overview of its code flow:

1. **Initialization**: The controller initializes by setting up the gRPC server and connecting to the Redpanda message queue.
2. **Event Handling**: When an event is received from a client, the controller publishes it to the message queue and updates its status in the database.
3. **Database Interaction**: The controller interacts with the database to store and retrieve event data.
4. **gRPC Routes**: The controller defines gRPC routes for clients to send events and check their status.

### Example Code

#### Initialization
The controller initializes the gRPC server and connects to the Redpanda message queue:

```go
package main

import (
	"log"
	"net"
	"google.golang.org/grpc"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	// Register services here

	log.Println("Starting gRPC server on port 50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
```

#### Event Handling
The controller publishes events to the Redpanda message queue:

```go
func publishEvent(event Event) error {
	// Logic to publish event to Redpanda
	return nil
}
```

#### Database Interaction
The controller updates the status of events in the database:

```go
func updateEventStatus(eventID string, status string) error {
	// Logic to update event status in the database
	return nil
}
```

#### gRPC Routes
The controller defines gRPC routes for clients to send events and check their status:

```go
func (s *server) SendEvent(ctx context.Context, req *SendEventRequest) (*SendEventResponse, error) {
	// Logic to handle SendEvent gRPC route
	return &SendEventResponse{}, nil
}
```