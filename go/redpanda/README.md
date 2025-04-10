# Redpanda

## Overview
Redpanda is a modern streaming platform designed for high-throughput and low-latency messaging. This project demonstrates its usage with a client, controller, and consumer setup.

## Components

### Client
The client produces events and checks their status. It connects to the Redpanda server and sends messages with various event types. The client also monitors the status of these events to ensure they are processed correctly.

### Controller
The controller manages the message queue by handling incoming messages, processing them, and routing them to the appropriate consumers. It also interacts with the database for event data management.

### Consumer
The consumer subscribes to specific topics in the Redpanda system. It processes messages as they arrive and performs actions based on the event type.

## Docker Compose
The `go-redpanda-compose.yml` file sets up the Redpanda environment. It includes the necessary services and configurations to run the Redpanda server, controller, and consumer. This simplifies the deployment and testing of the system.