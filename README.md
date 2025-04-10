# MessageQueueComparison

Highlight and compare different message and event streaming services

## Introduction

These are meant to be live examples of different message and event streaming services.  The goal is to provide a simple example of how to use the service, and to compare the performance of the different services.

## Services

The following services have examples in this project:

- [Redpanda](https://www.redpanda.com/)
- [NATS](https://nats.io/)

... more to come

## Languages

The comparison is done using the following programming languages:

- [Go](https://golang.org/)

... and more to come.

## Terminology

Due to the nature of the project, a comon terminology is utilized in an attempt to make the project easier to understand. 

The following terms are used:
- **namespace**: is a logical identifier for a grouping or region for the queue system. This can be utilized to isolate messages to a specific region.  Ex. `dev`, `test`, `prod`, `us`, `eu`, etc.  this is used to prefix streams, topics, and headers.
- **group_id**: is utilized to identify the services that is consuming the messages. Ex. `consumer1`, `consumer2`, etc.
- **topic**: is a subject or the identifier for the message. This is used to identify the message in the queue system. Ex. `event.us.created`, `event.us.updated`, etc.

## Process

### Client

- Utilizes a list of events to create randomized events.
- Each event is sent to the controller via gRPC.
  - controller returns an acknowledgment for the event.
  - controller returns a correlation ID for the new event.
  - correlation ID is saved in an inmemory map with the datetime of the event.
- Client checks for a status update for each event by calling the controller via gRPC.
  - when the event is returned completed, the client removes the event from the inmemory map, and logs out the time it took to complete the event.

### Controller

- Receives the event from the client via gRPC.
- Sends the event to the events message broker.



## TODO:
[] - Fix unit test, they are currently a mess and currently setup to just check things, they are not good examples of how to write test code.

