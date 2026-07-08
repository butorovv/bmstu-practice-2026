package config

import (
	"os"
	"time"
)

const (
	DefaultHTTPAddr           = ":8080"
	DefaultProcessingHTTPAddr = ":8081"
	DefaultShutdownTimeout    = 10 * time.Second
	DefaultPublisherBackend   = "log"
)

type Config struct {
	HTTPAddr         string
	ShutdownTimeout  time.Duration
	PublisherBackend string
}

type ProcessingConfig struct {
	HTTPAddr        string
	ShutdownTimeout time.Duration
}

func Load() Config {
	return Config{
		HTTPAddr:         getEnv("HTTP_ADDR", DefaultHTTPAddr),
		ShutdownTimeout:  DefaultShutdownTimeout,
		PublisherBackend: getEnv("PUBLISHER_BACKEND", DefaultPublisherBackend),
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
