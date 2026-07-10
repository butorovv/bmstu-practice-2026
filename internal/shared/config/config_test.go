package config

import (
	"reflect"
	"testing"
	"time"
)

func TestLoadCombinesKafkaAndRedisConfig(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":18080")
	t.Setenv("PUBLISHER_BACKEND", "kafka")
	t.Setenv("KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	t.Setenv("KAFKA_TELEMETRY_TOPIC", "telemetry.raw")
	t.Setenv("KAFKA_DLQ_TOPIC", "telemetry.dlq")
	t.Setenv("KAFKA_PUBLISH_TIMEOUT", "7s")
	t.Setenv("KAFKA_MAX_ATTEMPTS", "4")
	t.Setenv("KAFKA_MAX_IN_FLIGHT", "8")
	t.Setenv("REQUEST_TIMEOUT", "3s")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_DB", "2")

	cfg := Load()

	wantBrokers := []string{"kafka-1:9092", "kafka-2:9092"}
	if !reflect.DeepEqual(cfg.KafkaBrokers, wantBrokers) {
		t.Fatalf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, wantBrokers)
	}
	if !reflect.DeepEqual(cfg.Kafka.Brokers, wantBrokers) {
		t.Fatalf("Kafka.Brokers = %v, want %v", cfg.Kafka.Brokers, wantBrokers)
	}
	if cfg.Kafka.TelemetryTopic != "telemetry.raw" {
		t.Fatalf("Kafka.TelemetryTopic = %q", cfg.Kafka.TelemetryTopic)
	}
	if cfg.Kafka.DLQTopic != "telemetry.dlq" {
		t.Fatalf("Kafka.DLQTopic = %q", cfg.Kafka.DLQTopic)
	}
	if cfg.KafkaPublishTimeout != 7*time.Second {
		t.Fatalf("KafkaPublishTimeout = %v, want 7s", cfg.KafkaPublishTimeout)
	}
	if cfg.KafkaMaxAttempts != 4 {
		t.Fatalf("KafkaMaxAttempts = %d, want 4", cfg.KafkaMaxAttempts)
	}
	if cfg.KafkaMaxInFlight != 8 {
		t.Fatalf("KafkaMaxInFlight = %d, want 8", cfg.KafkaMaxInFlight)
	}
	if cfg.RequestTimeout != 3*time.Second {
		t.Fatalf("RequestTimeout = %v, want 3s", cfg.RequestTimeout)
	}
	if cfg.RedisAddr != "redis:6379" || cfg.RedisPassword != "secret" || cfg.RedisDB != 2 {
		t.Fatalf("Redis config = %q, %q, %d", cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	}
}

func TestLoadProcessingSupportsLegacyKafkaEnvNames(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("KAFKA_TELEMETRY_TOPIC", "")
	t.Setenv("KAFKA_RAW_TOPIC", "telemetry.raw")
	t.Setenv("KAFKA_DLQ_TOPIC", "telemetry.dlq")
	t.Setenv("KAFKA_CONSUMER_GROUP", "")
	t.Setenv("KAFKA_GROUP_ID", "processing-service")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_DB", "1")
	t.Setenv("POSTGRES_HOST", "postgres")
	t.Setenv("POSTGRES_PORT", "5433")
	t.Setenv("POSTGRES_DB", "processing")
	t.Setenv("POSTGRES_USER", "processing_user")
	t.Setenv("POSTGRES_PASSWORD", "processing_password")
	t.Setenv("POSTGRES_SSLMODE", "disable")
	t.Setenv("WINDOW_TTL", "6m")
	t.Setenv("ALERT_DEDUP_TTL", "7m")

	cfg := LoadProcessing()

	if !reflect.DeepEqual(cfg.Kafka.Brokers, []string{"kafka:9092"}) {
		t.Fatalf("Kafka.Brokers = %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.TelemetryTopic != "telemetry.raw" {
		t.Fatalf("Kafka.TelemetryTopic = %q", cfg.Kafka.TelemetryTopic)
	}
	if cfg.Kafka.DLQTopic != "telemetry.dlq" {
		t.Fatalf("Kafka.DLQTopic = %q", cfg.Kafka.DLQTopic)
	}
	if cfg.Kafka.ConsumerGroup != "processing-service" {
		t.Fatalf("Kafka.ConsumerGroup = %q", cfg.Kafka.ConsumerGroup)
	}
	if cfg.Redis.Addr != "redis:6379" || cfg.Redis.Password != "secret" || cfg.Redis.DB != 1 {
		t.Fatalf("Redis config = %q, %q, %d", cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	}
	if cfg.Postgres.Host != "postgres" ||
		cfg.Postgres.Port != 5433 ||
		cfg.Postgres.DB != "processing" ||
		cfg.Postgres.User != "processing_user" ||
		cfg.Postgres.Password != "processing_password" ||
		cfg.Postgres.SSLMode != "disable" {
		t.Fatalf("Postgres config = %+v", cfg.Postgres)
	}
	if cfg.Postgres.DSN() != "postgres://processing_user:processing_password@postgres:5433/processing?sslmode=disable" {
		t.Fatalf("Postgres DSN = %q", cfg.Postgres.DSN())
	}
	if cfg.WindowTTL != 6*time.Minute {
		t.Fatalf("WindowTTL = %v, want 6m", cfg.WindowTTL)
	}
	if cfg.AlertDedupTTL != 7*time.Minute {
		t.Fatalf("AlertDedupTTL = %v, want 7m", cfg.AlertDedupTTL)
	}
}

func TestDefaultKafkaBrokerMatchesDockerHostListener(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")

	cfg := Load()

	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"localhost:29092"}) {
		t.Fatalf("KafkaBrokers = %v, want [localhost:29092]", cfg.KafkaBrokers)
	}
}
