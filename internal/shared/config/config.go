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
	DefaultKafkaBrokers        = "localhost:9092"
	DefaultKafkaPublishTimeout = 5 * time.Second
)

type Config struct {
	HTTPAddr            string
	ShutdownTimeout     time.Duration
	PublisherBackend    string
	KafkaBrokers        []string
	KafkaPublishTimeout time.Duration
}

type ProcessingConfig struct {
	HTTPAddr        string
	ShutdownTimeout time.Duration
}

func Load() Config {
	return Config{
		HTTPAddr:            getEnv("HTTP_ADDR", DefaultHTTPAddr),
		ShutdownTimeout:     DefaultShutdownTimeout,
		PublisherBackend:    getEnv("PUBLISHER_BACKEND", DefaultPublisherBackend),
		KafkaBrokers:        splitCSV(getEnv("KAFKA_BROKERS", DefaultKafkaBrokers)),
		KafkaPublishTimeout: getDurationEnv("KAFKA_PUBLISH_TIMEOUT", DefaultKafkaPublishTimeout),
	}
}

func LoadProcessing() ProcessingConfig {
	return ProcessingConfig{
		HTTPAddr:        getEnv("PROCESSING_HTTP_ADDR", DefaultProcessingHTTPAddr),
		ShutdownTimeout: DefaultShutdownTimeout,
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fallback
	}

	return duration
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}
