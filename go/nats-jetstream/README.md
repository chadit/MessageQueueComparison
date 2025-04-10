# NATS JetStream

## Overview
NATS JetStream is a lightweight, high-performance messaging system. This project demonstrates its usage with a client, controller, and consumer setup.

## Components

### Client
The client is responsible for producing events and checking their status. It connects to the NATS JetStream server and sends messages with event types such as `create`, `deploy`, `clone`, `delete`, and `list`. The client also monitors the status of these events.

### Controller
The controller acts as the central hub for managing the message queue. It handles incoming messages, processes them, and routes them to the appropriate consumers. It also interacts with the database for storing and retrieving event data.

### Consumer
The consumer subscribes to specific topics or streams in the NATS JetStream system. It processes messages as they arrive and performs the necessary actions based on the event type.

## Docker Compose
The `go-nats-jetstream-compose.yml` file sets up the NATS JetStream environment. It includes the necessary services and configurations to run the NATS server, controller, and consumer. This allows for easy deployment and testing of the system.