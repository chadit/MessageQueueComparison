package eventstatus

import (
	"errors"
	"fmt"

	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

// ErrInvalidEventStatus is returned when an invalid status is provided.
var (
	ErrInvalidEventStatusString   = errors.New("invalid event status from string")
	ErrInvalidEventStatusInt      = errors.New("invalid event status from int")
	ErrInvalidEventStatusGrpc     = errors.New("invalid event status from gRPC")
	ErrInvalidEventStatusInternal = errors.New("invalid event status from internal")
)

// Status represents the status of an event.
type Status int

const (
	// Define the enum values matching the gRPC enum. e.g., enum EventStatus.
	EventStatusUnspecified Status = iota // 0
	EventStatusPending                   // 1
	EventStatusStarted                   // 2
	EventStatusFinished                  // 3
	EventStatusFailed                    // 4
)

// String returns the lowercase string representation of the EventStatus.
// For example, EventStatusPending becomes "pending".
func (e Status) String() string {
	switch e {
	case EventStatusUnspecified:
		return "unspecified"
	case EventStatusPending:
		return "pending"
	case EventStatusStarted:
		return "started"
	case EventStatusFinished:
		return "finished"
	case EventStatusFailed:
		return "failed"
	default:
		return fmt.Sprintf("EventStatus(%d)", int(e))
	}
}

// FromString converts a string to the corresponding EventStatus.
// It returns an error if the provided string does not map to a valid status.
func FromString(name string) (Status, error) {
	switch name {
	case "unspecified":
		return EventStatusUnspecified, nil
	case "pending":
		return EventStatusPending, nil
	case "started":
		return EventStatusStarted, nil
	case "finished":
		return EventStatusFinished, nil
	case "failed":
		return EventStatusFailed, nil
	default:
		return EventStatusUnspecified, fmt.Errorf("%w: %s", ErrInvalidEventStatusString, name)
	}
}

// FromInt validates an integer and converts it to EventStatus.
// It returns an error if the integer doesn't correspond to a valid status.
func FromInt(statusID int) (Status, error) {
	e := Status(statusID)
	switch e {
	case EventStatusUnspecified, EventStatusPending, EventStatusStarted, EventStatusFinished, EventStatusFailed:
		return e, nil
	default:
		return EventStatusUnspecified, fmt.Errorf("%w: %d", ErrInvalidEventStatusInt, statusID)
	}
}

// Mapping from internal EventStatus to gRPC EventStatus.
var internalToGrpc = map[Status]pb.EventStatus{
	EventStatusUnspecified: pb.EventStatus_EVENT_STATUS_UNSPECIFIED,
	EventStatusPending:     pb.EventStatus_EVENT_STATUS_PENDING,
	EventStatusStarted:     pb.EventStatus_EVENT_STATUS_STARTED,
	EventStatusFinished:    pb.EventStatus_EVENT_STATUS_FINISHED,
	EventStatusFailed:      pb.EventStatus_EVENT_STATUS_FAILED,
}

// Mapping from gRPC EventStatus to internal EventStatus.
var grpcToInternal = map[pb.EventStatus]Status{
	pb.EventStatus_EVENT_STATUS_UNSPECIFIED: EventStatusUnspecified,
	pb.EventStatus_EVENT_STATUS_PENDING:     EventStatusPending,
	pb.EventStatus_EVENT_STATUS_STARTED:     EventStatusStarted,
	pb.EventStatus_EVENT_STATUS_FINISHED:    EventStatusFinished,
	pb.EventStatus_EVENT_STATUS_FAILED:      EventStatusFailed,
}

// ToGrpc converts an internal EventStatus to its gRPC equivalent.
func ToGrpc(status Status) (pb.EventStatus, error) {
	if grpcStatus, ok := internalToGrpc[status]; ok {
		return grpcStatus, nil
	}

	return pb.EventStatus_EVENT_STATUS_UNSPECIFIED, fmt.Errorf("%w: %v", ErrInvalidEventStatusInternal, status)
}

// FromGrpc converts a gRPC EventStatus to its internal equivalent.
func FromGrpc(grpcStatus pb.EventStatus) (Status, error) {
	if status, ok := grpcToInternal[grpcStatus]; ok {
		return status, nil
	}

	return EventStatusUnspecified, fmt.Errorf("%w: %v", ErrInvalidEventStatusGrpc, grpcStatus)
}
