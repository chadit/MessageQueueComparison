package config

import (
	"os"
	"strings"
)

// GetBroker returns the Kafka broker address.
// This is utilized by Kafka and Kafka-based services. (e.g. redpanda).
func GetBroker(dnsName string) []string {
	if dnsName == "" {
		dnsName = "localhost"
	}

	var (
		brokers  string
		envExist bool
	)

	switch dnsName {
	case "nats":
		brokers, envExist = os.LookupEnv("NATS_BROKER")
	case "kafka", "redpanda":
		brokers, envExist = os.LookupEnv("KAFKA_BROKER")
	default:
		return []string{dnsName + ":" + DefaultKafkaPort}
	}

	if envExist {
		parts := strings.Split(brokers, ",")
		for i, broker := range parts {
			parts[i] = strings.TrimSpace(broker)
		}

		return parts
	}

	return []string{dnsName + ":" + DefaultKafkaPort}
}

// GetGRPCServerAddress returns the gRPC server address.
func GetGRPCServerAddress(dnsName string) string {
	if dnsName == "" {
		dnsName = "localhost"
	}

	if v, ok := os.LookupEnv("GRPC_SERVER_ADDRESS"); ok {
		return v
	}

	return dnsName + ":" + DefaultGRPCPort
}

func GetRequestTopic() string {
	if v, ok := os.LookupEnv("REQUEST_TOPIC"); ok {
		return v
	}

	return "event"
}

func GetGroupID() string {
	if v, ok := os.LookupEnv("GROUP_ID"); ok {
		return v
	}

	return "event.processing"
}

// GetNamespace returns the name of service.
// this can be something like "hostname" or "service name".
// this is used to identify the service for worker queues.
func GetNamespace() string {
	if v, ok := os.LookupEnv("NAMESPACE"); ok {
		return v
	}

	return "default"
}

func GetSupportedEvents() string {
	if v, ok := os.LookupEnv("SUPPORTED_EVENTS"); ok {
		return v
	}

	return "create,deploy,clone,delete,list"
}

func GetIfSimulateFailure() bool {
	if v, ok := os.LookupEnv("SIMULATE_FAILURE"); ok {
		return v == "true"
	}

	return false
}
