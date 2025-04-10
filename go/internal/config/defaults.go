package config

import "time"

const (
	DefaultRedPandaDNSName          = "redpanda"
	DefaultNatsDNSName              = "nats"
	DefaultKafkaPort                = "9092"
	DefaultGRPCPort                 = "50051"
	DefaultRequestTopic             = "event"
	DefaultStatusTopic              = "status"
	DefaultMaxMessageSize           = 1024 * 1024 * 10 // 10 MB
	DefaultErrorLoopBackoffInterval = 2 * time.Second
	DefaultTimeoutToWriteMessage    = 120 * time.Second
	DefaultTimeoutReadMessage       = 30 * time.Second
	DefaultTimeoutHealthCheck       = 5 * time.Second

	DefaultEventGroupID  = "event.processing"
	DefaultStatusGroupID = "status.processing"
)
