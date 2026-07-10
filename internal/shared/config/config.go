package config

import (
	"net"
	"net/url"
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
	DefaultKafkaBrokers        = "localhost:29092"
	DefaultKafkaTelemetryTopic = "telemetry.raw"
	DefaultKafkaDLQTopic       = "telemetry.dlq"
	DefaultKafkaConsumerGroup  = "processing-service"
	DefaultKafkaPublishTimeout = 5 * time.Second
	DefaultKafkaMaxAttempts    = 5
	DefaultKafkaMaxInFlight    = 32
	DefaultRequestTimeout      = 10 * time.Second
	DefaultRedisAddr           = "localhost:6379"
	DefaultPostgresHost        = "localhost"
	DefaultPostgresPort        = 5432
	DefaultPostgresDB          = "telemetry"
	DefaultPostgresUser        = "postgres"
	DefaultPostgresPassword    = "postgres"
	DefaultPostgresSSLMode     = "disable"
	DefaultWindowTTL           = 5 * time.Minute
	DefaultAlertDedupTTL       = 5 * time.Minute
)

type Config struct {
	HTTPAddr            string
	ShutdownTimeout     time.Duration
	PublisherBackend    string
	Kafka               KafkaConfig
	KafkaBrokers        []string
	KafkaPublishTimeout time.Duration
	KafkaMaxAttempts    int
	KafkaMaxInFlight    int
	RequestTimeout      time.Duration
	RedisAddr           string
	RedisPassword       string
	RedisDB             int
}

type ProcessingConfig struct {
	HTTPAddr        string
	ShutdownTimeout time.Duration
	Kafka           KafkaConfig
	Redis           RedisConfig
	Postgres        PostgresConfig
	WindowTTL       time.Duration
	AlertDedupTTL   time.Duration
}

type KafkaConfig struct {
	Brokers        []string
	TelemetryTopic string
	DLQTopic       string
	ConsumerGroup  string
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type PostgresConfig struct {
	Host     string
	Port     int
	DB       string
	User     string
	Password string
	SSLMode  string
}

func (c PostgresConfig) DSN() string {
	user := url.User(c.User)
	if c.Password != "" {
		user = url.UserPassword(c.User, c.Password)
	}

	dsn := url.URL{
		Scheme: "postgres",
		User:   user,
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:   c.DB,
	}

	query := dsn.Query()
	query.Set("sslmode", c.SSLMode)
	dsn.RawQuery = query.Encode()

	return dsn.String()
}

func Load() Config {
	kafkaConfig := loadKafkaConfig()
	redisConfig := loadRedisConfig()

	return Config{
		HTTPAddr:            getEnv("HTTP_ADDR", DefaultHTTPAddr),
		ShutdownTimeout:     DefaultShutdownTimeout,
		PublisherBackend:    getEnv("PUBLISHER_BACKEND", DefaultPublisherBackend),
		Kafka:               kafkaConfig,
		KafkaBrokers:        kafkaConfig.Brokers,
		KafkaPublishTimeout: getDurationEnv("KAFKA_PUBLISH_TIMEOUT", DefaultKafkaPublishTimeout),
		KafkaMaxAttempts:    getIntEnv("KAFKA_MAX_ATTEMPTS", DefaultKafkaMaxAttempts),
		KafkaMaxInFlight:    getIntEnv("KAFKA_MAX_IN_FLIGHT", DefaultKafkaMaxInFlight),
		RequestTimeout:      getDurationEnv("REQUEST_TIMEOUT", DefaultRequestTimeout),
		RedisAddr:           redisConfig.Addr,
		RedisPassword:       redisConfig.Password,
		RedisDB:             redisConfig.DB,
	}
}

func LoadProcessing() ProcessingConfig {
	return ProcessingConfig{
		HTTPAddr:        getEnv("PROCESSING_HTTP_ADDR", DefaultProcessingHTTPAddr),
		ShutdownTimeout: DefaultShutdownTimeout,
		Kafka:           loadKafkaConfig(),
		Redis:           loadRedisConfig(),
		Postgres:        loadPostgresConfig(),
		WindowTTL:       getDurationEnv("WINDOW_TTL", DefaultWindowTTL),
		AlertDedupTTL:   getDurationEnv("ALERT_DEDUP_TTL", DefaultAlertDedupTTL),
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
		DLQTopic:       getEnv("KAFKA_DLQ_TOPIC", DefaultKafkaDLQTopic),
		ConsumerGroup:  getEnv("KAFKA_CONSUMER_GROUP", getEnv("KAFKA_GROUP_ID", DefaultKafkaConsumerGroup)),
	}
}

func loadRedisConfig() RedisConfig {
	return RedisConfig{
		Addr:     getEnv("REDIS_ADDR", DefaultRedisAddr),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       getIntEnv("REDIS_DB", 0),
	}
}

func loadPostgresConfig() PostgresConfig {
	return PostgresConfig{
		Host:     getEnv("POSTGRES_HOST", DefaultPostgresHost),
		Port:     getIntEnv("POSTGRES_PORT", DefaultPostgresPort),
		DB:       getEnv("POSTGRES_DB", DefaultPostgresDB),
		User:     getEnv("POSTGRES_USER", DefaultPostgresUser),
		Password: getEnv("POSTGRES_PASSWORD", DefaultPostgresPassword),
		SSLMode:  getEnv("POSTGRES_SSLMODE", DefaultPostgresSSLMode),
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
