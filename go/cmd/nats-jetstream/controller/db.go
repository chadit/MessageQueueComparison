package main

import (
	"errors"
	"fmt"

	"github.com/hashicorp/go-memdb"
)

const (
	EventsDBName = "events"
)

var (
	ErrEventNotFound       = errors.New("event not found in database")
	ErrEventNotCorrectType = errors.New("event is not of type EventTable")
)

type EventTable struct {
	CorrelationID string
	Status        int
	EventType     string
}

// Create a new database.
func NewDB() (*memdb.MemDB, error) {
	// Create the DB schema
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			EventsDBName: {
				Name: EventsDBName,
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "CorrelationID"},
					},
					"eventtype": {
						Name:    "eventtype",
						Unique:  false,
						Indexer: &memdb.StringFieldIndex{Field: "EventType"},
					},
					"status": {
						Name:    "status",
						Unique:  false,
						Indexer: &memdb.IntFieldIndex{Field: "Status"},
					},
				},
			},
		},
	}

	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	return db, nil
}

// Insert a new event into the database.
func InsertEvent(db *memdb.MemDB, event *EventTable) error {
	txn := db.Txn(true)
	defer txn.Abort()

	if err := txn.Insert(EventsDBName, event); err != nil {
		return fmt.Errorf("failed to insert event into database: %w", err)
	}

	txn.Commit()

	return nil
}

// GetEvent returns an event from the database.
func GetEvent(db *memdb.MemDB, correlationID string) (*EventTable, error) {
	txn := db.Txn(false)
	defer txn.Abort()

	raw, err := txn.First(EventsDBName, "id", correlationID)
	if err != nil {
		return nil, fmt.Errorf("failed to get event from database: %w", err)
	}

	if raw == nil {
		return nil, ErrEventNotFound
	}

	event, isValid := raw.(*EventTable)
	if !isValid {
		return nil, ErrEventNotCorrectType
	}

	return event, nil
}
