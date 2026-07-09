package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultHTTPAddr            = ":8080"
	DefaultProcessingHTTPAddr  = ":8081"
	DefaultShutdownTimeout     = 10 * time.Second
	DefaultPublisherBackend    = "log"
	DefaultKafkaBrokers        = "localhost:9092"
	DefaultKafkaTelemetryTopic = "telemetry.raw"
	DefaultKafkaConsumerGroup  = "processing-service"
	DefaultKafkaPublishTimeout = 5 * time.Second
	DefaultRedisAddr           = "localhost:6379"
)

type Config struct {
	HTTPAddr            string
	ShutdownTimeout     time.Duration
	PublisherBackend    string
	Kafka               KafkaConfig
	KafkaBrokers        []string
	KafkaPublishTimeout time.Duration
	RedisAddr           string
	RedisPassword       string
	RedisDB             int
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
	kafkaConfig := loadKafkaConfig()

	return Config{
		HTTPAddr:            getEnv("HTTP_ADDR", DefaultHTTPAddr),
		ShutdownTimeout:     DefaultShutdownTimeout,
		PublisherBackend:    getEnv("PUBLISHER_BACKEND", DefaultPublisherBackend),
		Kafka:               kafkaConfig,
		KafkaBrokers:        kafkaConfig.Brokers,
		KafkaPublishTimeout: getDurationEnv("KAFKA_PUBLISH_TIMEOUT", DefaultKafkaPublishTimeout),
		RedisAddr:           getEnv("REDIS_ADDR", DefaultRedisAddr),
		RedisPassword:       os.Getenv("REDIS_PASSWORD"),
		RedisDB:             getIntEnv("REDIS_DB", 0),
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

func getIntEnv(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	number, err := strconv.Atoi(value)
	if err != nil || number < 0 {
		return fallback
	}

	return number
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}

	if len(result) == 0 {
		return []string{DefaultKafkaBrokers}
	}

	return result
}
