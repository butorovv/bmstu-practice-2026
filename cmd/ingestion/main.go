package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/delivery"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	redisrepo "github.com/butorovv/bmstu-practice-2026/internal/ingestion/repository/redis"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
	"github.com/butorovv/bmstu-practice-2026/internal/shared/config"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()
	pub, err := newPublisher(cfg, logger)
	if err != nil {
		logger.Error("failed to create publisher", "error", err)
		os.Exit(1)
	}

	redisClient := redisrepo.NewClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	idempotencyRepository := redisrepo.NewIdempotencyRepository(redisClient)
	rateLimiter := redisrepo.NewRateLimiter(redisClient)
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	metrics := delivery.NewMetrics(registry)
	readinessChecks := map[string]delivery.ReadinessCheck{
		"redis": func(ctx context.Context) error {
			return redisClient.Ping(ctx).Err()
		},
	}
	if readinessPublisher, ok := pub.(interface {
		Ready(context.Context) error
	}); ok {
		readinessChecks["kafka"] = readinessPublisher.Ready
	}

	handler := delivery.NewHandlerWithOptions(
		pub,
		validator.New(),
		idempotencyRepository,
		rateLimiter,
		delivery.HandlerOptions{
			RequestTimeout:   cfg.RequestTimeout,
			ReadinessTimeout: cfg.ReadinessTimeout,
			ReadinessChecks:  readinessChecks,
			Logger:           logger,
			Metrics:          metrics,
		},
	)
	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: delivery.NewRouter(handler, delivery.MetricsHandler(registry)),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info(
			"starting ingestion service",
			"address", cfg.HTTPAddr,
			"publisher", cfg.PublisherBackend,
		)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErrors <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-serverErrors:
		logger.Error("ingestion HTTP server stopped", "error", err)
	}
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("ingestion service shutdown failed", "error", err)
	}
	if err := closePublisher(pub); err != nil {
		logger.Error("ingestion publisher close failed", "error", err)
	}
	if err := redisClient.Close(); err != nil {
		logger.Error("ingestion redis client close failed", "error", err)
	}

	logger.Info("ingestion service stopped")
}

func newPublisher(cfg config.Config, logger *slog.Logger) (publisher.Publisher, error) {
	switch cfg.PublisherBackend {
	case "", config.DefaultPublisherBackend:
		return publisher.NewLogPublisher(), nil
	case "kafka":
		logger.Info(
			"using Kafka publisher",
			"brokers", cfg.KafkaBrokers,
			"topic", publisher.TelemetryRawTopic,
		)
		return publisher.NewKafkaPublisherWithConfig(publisher.KafkaPublisherConfig{
			Brokers:        cfg.KafkaBrokers,
			PublishTimeout: cfg.KafkaPublishTimeout,
			MaxAttempts:    cfg.KafkaMaxAttempts,
			MaxInFlight:    cfg.KafkaMaxInFlight,
		})
	default:
		return nil, fmt.Errorf("unsupported PUBLISHER_BACKEND %q", cfg.PublisherBackend)
	}
}

func closePublisher(pub publisher.Publisher) error {
	closer, ok := pub.(interface {
		Close() error
	})
	if !ok {
		return nil
	}

	return closer.Close()
}
