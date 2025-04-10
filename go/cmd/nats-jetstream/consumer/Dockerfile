# Build stage
FROM golang:latest AS builder
WORKDIR /app
# Copy go.mod and the go directory so that dependencies and code are available.
COPY go.mod go.sum ./

RUN go mod download

# Install protoc on Debian/Ubuntu base images
RUN apt-get update && apt-get install -y protobuf-compiler
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Now copy all source files, including internal packages and generated proto.
COPY . .

# (Optional) If you need to generate proto inside the container, do it here:
  RUN protoc --go_out=go/internal/proto \
  --go-grpc_out=go/internal/proto \
  --go_opt=paths=source_relative \
  --go-grpc_opt=paths=source_relative \
  proto/message.proto

# Double-check dependencies in case any new imports are introduced.
RUN go mod tidy

# Build the consumer binary.
WORKDIR /app/go/cmd/nats-jetstream/consumer
RUN CGO_ENABLED=0 GOOS=linux go build -o consumer main.go

# Run stage
FROM debian:stable-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /root/
COPY --from=builder /app/go/cmd/nats-jetstream/consumer/consumer .
EXPOSE 50051
CMD ["./consumer"]