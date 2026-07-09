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
	t.Setenv("KAFKA_PUBLISH_TIMEOUT", "7s")
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
	if cfg.KafkaPublishTimeout != 7*time.Second {
		t.Fatalf("KafkaPublishTimeout = %v, want 7s", cfg.KafkaPublishTimeout)
	}
	if cfg.RedisAddr != "redis:6379" || cfg.RedisPassword != "secret" || cfg.RedisDB != 2 {
		t.Fatalf("Redis config = %q, %q, %d", cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	}
}

func TestLoadProcessingSupportsLegacyKafkaEnvNames(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("KAFKA_TELEMETRY_TOPIC", "")
	t.Setenv("KAFKA_RAW_TOPIC", "telemetry.raw")
	t.Setenv("KAFKA_CONSUMER_GROUP", "")
	t.Setenv("KAFKA_GROUP_ID", "processing-service")

	cfg := LoadProcessing()

	if !reflect.DeepEqual(cfg.Kafka.Brokers, []string{"kafka:9092"}) {
		t.Fatalf("Kafka.Brokers = %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.TelemetryTopic != "telemetry.raw" {
		t.Fatalf("Kafka.TelemetryTopic = %q", cfg.Kafka.TelemetryTopic)
	}
	if cfg.Kafka.ConsumerGroup != "processing-service" {
		t.Fatalf("Kafka.ConsumerGroup = %q", cfg.Kafka.ConsumerGroup)
	}
}

func TestDefaultKafkaBrokerMatchesDockerHostListener(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")

	cfg := Load()

	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"localhost:29092"}) {
		t.Fatalf("KafkaBrokers = %v, want [localhost:29092]", cfg.KafkaBrokers)
	}
}
