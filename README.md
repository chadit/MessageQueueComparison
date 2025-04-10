# Message Queue Comparison

## Overview
This repository provides live examples of different message and event streaming services. The goal is to demonstrate how to use these services and compare their performance.

## Supported Services
- [NATS JetStream](go/cmd/nats-jetstream/README.md): Learn more about the NATS JetStream implementation, including its components and usage.
- [Redpanda](go/cmd/redpanda/README.md): Learn more about the Redpanda implementation, including its components and usage.

## Programming Languages
- [Go](https://golang.org/)

## Terminology
To maintain consistency across the project, the following terms are used:
- **namespace**: A logical identifier for grouping or isolating messages (e.g., `dev`, `test`, `prod`, `us`, `eu`). Used to prefix streams, topics, and headers.
- **group_id**: Identifies the service consuming the messages (e.g., `consumer1`, `consumer2`).
- **topic**: A subject or identifier for the message (e.g., `event.us.created`, `event.us.updated`).

## Process

### Client
- Generates randomized events from a predefined list.
- Sends events to the controller via gRPC.
  - Receives acknowledgment and a correlation ID for each event.
  - Stores the correlation ID in an in-memory map with the event's timestamp.
- Periodically checks the status of each event by querying the controller via gRPC.
  - Logs the time taken to complete the event once marked as completed.

### Controller
- Receives events from the client via gRPC.
- Publishes events to the message broker.
- Manages event status and interacts with the database.

### Consumer
- Subscribes to topics or streams.
- Processes incoming messages in real-time.
- Executes actions based on the event type.

## Deployment
The project includes Docker Compose files for setting up the environments:
- `go-nats-jetstream-compose.yml`: Sets up the NATS JetStream environment.
- `go-redpanda-compose.yml`: Sets up the Redpanda environment.

## TODO
- [ ] Improve unit tests to serve as better examples of test code.

