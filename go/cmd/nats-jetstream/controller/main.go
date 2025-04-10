package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/logger"
	"github.com/chadit/StreamProcessorsComparison/go/internal/natsqueue"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

// Controller implements the gRPC MessagingService.
type Controller struct {
	// using unsafe to force routes to be implemented.
	pb.UnsafeMessagingServiceServer
	// brokers string
	logger *slog.Logger
	db     *memdb.MemDB
	nats   jetstream.JetStream
}

// New initializes the message service controller plain.
func NewController(ctx context.Context, logLevel slog.Leveler, logOutput io.Writer, timeout time.Duration) (*Controller, error) {
	srv := Controller{
		logger: logger.New(logLevel, logOutput),
	}

	var err error

	brokers := strings.Join(config.GetBroker("nats"), ",")

	// create nats jetstream.
	if srv.nats, err = natsqueue.NewConnection(brokers); err != nil {
		return nil, fmt.Errorf("failed to create nats jetstream: %w", err)
	}

	srv.logger.DebugContext(ctx, "message broker connected successful", "broker", brokers)

	// setup database connection.
	if srv.db, err = NewDB(); err != nil {
		return &srv, fmt.Errorf("failed to create database: %w", err)
	}

	// Wait for message service to be ready before starting the gRPC server.
	if err = srv.waitForMessageQueue(ctx, timeout); err != nil {
		return &srv, fmt.Errorf("queue not available: %w", err)
	}

	return &srv, nil
}

// Close closes the nats connection.
func (c *Controller) Close() {
	if c.nats != nil && c.nats.Conn() != nil {
		c.nats.Conn().Close()
	}
}

func main() {
	ctx := context.Background()

	logLevel := slog.LevelDebug
	logOutput := os.Stdout

	srv, err := NewController(ctx, logLevel, logOutput, 1*time.Minute)
	if err != nil {
		srv.logger.ErrorContext(ctx, "failed to start server", "error", err)

		os.Exit(1)
	}

	// closes the message service when application closes.
	defer srv.Close()

	// Kick off the periodic health check.
	srv.startMessageServiceHealthCheck(ctx)

	// // Start topic reporting.
	if err = srv.startQueueReport(ctx); err != nil {
		srv.logger.ErrorContext(ctx, "failed to start topic reporting", "error", err)

		return
	}

	// Listen for status updates from the message service.
	if err = srv.listenForStatusUpdate(ctx); err != nil {
		srv.logger.ErrorContext(ctx, "failed to listen for status updates", "error", err)

		return
	}

	srv.logger.DebugContext(ctx, "starting gRPC server", "address", "tcp:50051")

	//nolint: gosec // this is not a security issue as this is for testing purposes.
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		srv.logger.ErrorContext(ctx, "failed to start tcp listener", "error", err)

		return
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMessagingServiceServer(grpcServer, srv)

	srv.logger.InfoContext(ctx, "gRPC server started", "address", "tcp:50051")

	if err := grpcServer.Serve(lis); err != nil {
		srv.logger.ErrorContext(ctx, "failed to start grpc server", "error", err)

		return
	}
}
