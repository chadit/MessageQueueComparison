package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/hashicorp/go-memdb"
	"google.golang.org/grpc"

	"github.com/chadit/StreamProcessorsComparison/go/internal/config"
	"github.com/chadit/StreamProcessorsComparison/go/internal/logger"
	pb "github.com/chadit/StreamProcessorsComparison/go/internal/proto"
)

// Controller implements the gRPC MessagingService.
type Controller struct {
	// using unsafe to force routes to be implemented.
	pb.UnsafeMessagingServiceServer
	brokers []string
	logger  *slog.Logger
	db      *memdb.MemDB
}

// New initializes the message service controller plain.
func NewController(ctx context.Context, logLevel slog.Leveler, logOutput io.Writer, timeout time.Duration) (*Controller, error) {
	srv := Controller{
		brokers: config.GetBroker("redpanda"),
		logger:  logger.New(logLevel, logOutput),
	}

	var err error

	// setup database connection.
	if srv.db, err = NewDB(); err != nil {
		return &srv, fmt.Errorf("failed to create database: %w", err)
	}

	// Wait for Kafka to be ready before starting the gRPC server. 1*time.Minute
	if err = srv.waitForMessageQueue(ctx, timeout); err != nil {
		return &srv, fmt.Errorf("queue not available: %w", err)
	}

	return &srv, nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logLevel := slog.LevelDebug
	logOutput := os.Stdout

	srv, err := NewController(ctx, logLevel, logOutput, 1*time.Minute)
	if err != nil {
		srv.logger.ErrorContext(ctx, "failed to start server", "error", err)

		return
	}

	srv.logger.DebugContext(ctx, "message queue started successfully", "broker", srv.brokers)

	// Kick off the periodic health check.
	srv.startMessageServiceHealthCheck(ctx)

	// Start topic reporting.
	srv.startQueueReport(ctx)

	// Listen for status updates from the message service.
	if err = srv.listenForStatusUpdate(ctx); err != nil {
		srv.logger.ErrorContext(ctx, "failed to listen for status updates", "error", err)

		return
	}

	srv.logger.DebugContext(ctx, "starting gRPC server", "address", "tcp:50051")

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

	srv.logger.DebugContext(ctx, "grpc service stopped")

	// wait for the context to be done.
	<-ctx.Done()
}
