.PHONY: go-proto go-redpanda-docker-up go-redpanda-docker-down go-nats-docker-up go-nats-docker-down

go-proto:
	protoc --go_out=go/internal/proto --go-grpc_out=go/internal/proto proto/message.proto

go-redpanda-docker-up:
	docker-compose -f go-redpanda-compose.yml up --build

go-redpanda-docker-down:
	# passing -v to remove volumes
	docker-compose -f go-redpanda-compose.yml down -v

go-nats-docker-up:
	docker-compose -f go-nats-jetstream-compose.yml up --build

go-nats-docker-down:
	# passing -v to remove volumes
	docker-compose -f go-nats-jetstream-compose.yml down -v