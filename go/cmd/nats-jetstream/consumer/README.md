# NATS Consumer

## Overview
The NATS Consumer is a component of the NATS JetStream messaging system. It subscribes to specific topics or streams and processes messages as they arrive. The consumer performs the necessary actions based on the event type, ensuring that the system operates efficiently and reliably.

## Features
- Subscribes to NATS JetStream topics or streams.
- Processes incoming messages in real-time.
- Executes actions based on the event type.
- Ensures reliable message handling and processing.

## Usage
The NATS Consumer is designed to work seamlessly with the NATS JetStream Controller and Client. It listens for messages published to the NATS JetStream server and processes them accordingly.

## Docker Integration
The NATS Consumer can be deployed using Docker. The provided `Dockerfile` contains the necessary instructions to build a Docker image for the consumer. This allows for easy deployment and scaling in a containerized environment.

## Code Structure
- `main.go`: The entry point for the NATS Consumer application. It initializes the consumer and starts listening for messages.

## Prerequisites
- A running instance of NATS JetStream.
- Properly configured topics or streams for the consumer to subscribe to.

## Deployment
To deploy the NATS Consumer, build the Docker image using the provided `Dockerfile` and run it in a containerized environment. Ensure that the NATS JetStream server is accessible and properly configured.

## Code Flow

The NATS Consumer is responsible for subscribing to topics or streams and processing messages in real-time. Below is an overview of its code flow:

1. **Initialization**: The consumer initializes by connecting to the NATS JetStream server and setting up subscriptions to specific topics.
2. **Message Handling**: When a message is received, the consumer processes it based on its content and performs the necessary actions.
3. **Error Handling**: The consumer includes mechanisms to handle errors during subscription and message processing.
4. **Graceful Shutdown**: The consumer ensures a clean shutdown by closing its connection to the NATS server.

### Example Code

#### Initialization
The consumer connects to the NATS server and subscribes to a topic:

```go
nc, err := nats.Connect(nats.DefaultURL)
if err != nil {
	log.Fatalf("Error connecting to NATS: %v", err)
}
defer nc.Close()

sub, err := nc.Subscribe("example.topic", func(msg *nats.Msg) {
	log.Printf("Received message: %s", string(msg.Data))
})
if err != nil {
	log.Fatalf("Error subscribing to topic: %v", err)
}
defer sub.Unsubscribe()
```

#### Message Handling
The consumer processes messages as they arrive:

```go
func processMessage(msg *nats.Msg) {
	log.Printf("Processing message: %s", string(msg.Data))
}
```

#### Error Handling
Errors during subscription or message processing are logged and handled appropriately:

```go
sub, err := nc.Subscribe("example.topic", func(msg *nats.Msg) {
	if err := processMessage(msg); err != nil {
		log.Printf("Error processing message: %v", err)
	}
})
if err != nil {
	log.Fatalf("Error subscribing to topic: %v", err)
}
```

#### Graceful Shutdown
The consumer ensures a clean shutdown by closing its connection to the NATS server:

```go
nc, err := nats.Connect(nats.DefaultURL)
if err != nil {
	log.Fatalf("Error connecting to NATS: %v", err)
}
defer nc.Close()
```