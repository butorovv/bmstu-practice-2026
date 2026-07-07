package config

import (
	"os"
	"time"
)

const (
	DefaultHTTPAddr         = ":8080"
	DefaultShutdownTimeout  = 10 * time.Second
	DefaultPublisherBackend = "log"
)

type Config struct {
	HTTPAddr         string
	ShutdownTimeout  time.Duration
	PublisherBackend string
}

func Load() Config {
	return Config{
		HTTPAddr:         getEnv("HTTP_ADDR", DefaultHTTPAddr),
		ShutdownTimeout:  DefaultShutdownTimeout,
		PublisherBackend: getEnv("PUBLISHER_BACKEND", DefaultPublisherBackend),
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
