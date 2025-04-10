# Redpanda Consumer

## Overview
The Redpanda Consumer is a component of the Redpanda messaging system. It subscribes to specific topics and processes messages as they arrive. The consumer performs the necessary actions based on the event type, ensuring that the system operates efficiently and reliably.

## Features
- Subscribes to Redpanda topics.
- Processes incoming messages in real-time.
- Executes actions based on the event type.
- Ensures reliable message handling and processing.

## Usage
The Redpanda Consumer is designed to work seamlessly with the Redpanda Controller and Client. It listens for messages published to the Redpanda server and processes them accordingly.

## Docker Integration
The Redpanda Consumer can be deployed using Docker. The provided `Dockerfile` contains the necessary instructions to build a Docker image for the consumer. This allows for easy deployment and scaling in a containerized environment.

## Code Structure
- `main.go`: The entry point for the Redpanda Consumer application. It initializes the consumer and starts listening for messages.

## Prerequisites
- A running instance of Redpanda.
- Properly configured topics for the consumer to subscribe to.

## Deployment
To deploy the Redpanda Consumer, build the Docker image using the provided `Dockerfile` and run it in a containerized environment. Ensure that the Redpanda server is accessible and properly configured.

## Code Flow

The Redpanda Consumer is responsible for subscribing to topics and processing messages in real-time. Below is an overview of its code flow:

1. **Initialization**: The consumer initializes by connecting to the Redpanda server and setting up subscriptions to specific topics.
2. **Message Handling**: When a message is received, the consumer processes it based on its content and performs the necessary actions.
3. **Error Handling**: The consumer includes mechanisms to handle errors during subscription and message processing.
4. **Graceful Shutdown**: The consumer ensures a clean shutdown by closing its connection to the Redpanda server.

### Example Code

#### Initialization
The consumer connects to the Redpanda server and subscribes to a topic:

```go
package main

import (
	"log"
	"github.com/segmentio/kafka-go"
)

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "example.topic",
		GroupID: "example-group",
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("Error reading message: %v", err)
		}
		log.Printf("Received message: %s", string(m.Value))
	}
}
```

#### Message Handling
The consumer processes messages as they arrive:

```go
func processMessage(msg kafka.Message) {
	log.Printf("Processing message: %s", string(msg.Value))
}
```

#### Error Handling
Errors during subscription or message processing are logged and handled appropriately:

```go
for {
	m, err := r.ReadMessage(context.Background())
	if err != nil {
		log.Printf("Error reading message: %v", err)
		continue
	}
	processMessage(m)
}
```

#### Graceful Shutdown
The consumer ensures a clean shutdown by closing its connection to the Redpanda server:

```go
r := kafka.NewReader(kafka.ReaderConfig{
	Brokers: []string{"localhost:9092"},
	Topic:   "example.topic",
	GroupID: "example-group",
})
defer r.Close()
```