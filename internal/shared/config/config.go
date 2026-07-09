package config

import (
	"os"
	"strings"
	"time"
)

const (
	DefaultHTTPAddr            = ":8080"
	DefaultProcessingHTTPAddr  = ":8081"
	DefaultShutdownTimeout     = 10 * time.Second
	DefaultPublisherBackend    = "log"
	DefaultKafkaBrokers        = "kafka:9092"
	DefaultKafkaTelemetryTopic = "telemetry.raw"
	DefaultKafkaConsumerGroup  = "processing-service"
)

type Config struct {
	HTTPAddr         string
	ShutdownTimeout  time.Duration
	PublisherBackend string
	Kafka            KafkaConfig
}

type ProcessingConfig struct {
	HTTPAddr        string
	ShutdownTimeout time.Duration
	Kafka           KafkaConfig
}

type KafkaConfig struct {
	Brokers        []string
	TelemetryTopic string
	ConsumerGroup  string
}

func Load() Config {
	return Config{
		HTTPAddr:         getEnv("HTTP_ADDR", DefaultHTTPAddr),
		ShutdownTimeout:  DefaultShutdownTimeout,
		PublisherBackend: getEnv("PUBLISHER_BACKEND", DefaultPublisherBackend),
		Kafka:            loadKafkaConfig(),
	}
}

func LoadProcessing() ProcessingConfig {
	return ProcessingConfig{
		HTTPAddr:        getEnv("PROCESSING_HTTP_ADDR", DefaultProcessingHTTPAddr),
		ShutdownTimeout: DefaultShutdownTimeout,
		Kafka:           loadKafkaConfig(),
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func loadKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Brokers:        splitCSV(getEnv("KAFKA_BROKERS", DefaultKafkaBrokers)),
		TelemetryTopic: getEnv("KAFKA_TELEMETRY_TOPIC", getEnv("KAFKA_RAW_TOPIC", DefaultKafkaTelemetryTopic)),
		ConsumerGroup:  getEnv("KAFKA_CONSUMER_GROUP", getEnv("KAFKA_GROUP_ID", DefaultKafkaConsumerGroup)),
	}
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}

	if len(values) == 0 {
		return []string{DefaultKafkaBrokers}
	}

	return values
}
