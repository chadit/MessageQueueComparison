# Redpanda

## Overview
Redpanda is a modern streaming platform designed for high-throughput and low-latency messaging. This project demonstrates its usage with a client, controller, and consumer setup.

For more information about Redpanda, visit the [official Redpanda website](https://redpanda.com/).

## Components

### Client
The client is responsible for producing events and checking their status. It connects to the Redpanda server and sends messages with event types such as `create`, `deploy`, `clone`, `delete`, and `list`. The client also monitors the status of these events.

For more details, refer to the [Client README](../client/README.md).

### Controller
The Redpanda Controller acts as the central hub for managing the Redpanda message queue. It handles incoming messages, processes them, and routes them to the appropriate consumers. For more details, refer to the [Controller README](controller/README.md).

### Consumer
The Redpanda Consumer subscribes to specific topics and processes messages in real-time. For more details, refer to the [Consumer README](consumer/README.md).

## Docker Compose
The `go-redpanda-compose.yml` file sets up the Redpanda environment. It includes the necessary services and configurations to run the Redpanda server, controller, and consumer. This simplifies the deployment and testing of the system.