package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/delivery"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	redisrepo "github.com/butorovv/bmstu-practice-2026/internal/ingestion/repository/redis"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
	"github.com/butorovv/bmstu-practice-2026/internal/shared/config"
)

func main() {
	cfg := config.Load()
	pub, err := newPublisher(cfg)
	if err != nil {
		log.Fatalf("failed to create publisher: %v", err)
	}

	redisClient := redisrepo.NewClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	idempotencyRepository := redisrepo.NewIdempotencyRepository(redisClient)
	rateLimiter := redisrepo.NewRateLimiter(redisClient)

	handler := delivery.NewHandlerWithOptions(
		pub,
		validator.New(),
		idempotencyRepository,
		rateLimiter,
		delivery.HandlerOptions{RequestTimeout: cfg.RequestTimeout},
	)
	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: delivery.NewRouter(handler),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("starting ingestion service on %s with publisher=%s", cfg.HTTPAddr, cfg.PublisherBackend)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ingestion service stopped: %v", err)
		}
	}()

	<-ctx.Done()
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("ingestion service shutdown failed: %v", err)
	}
	if err := closePublisher(pub); err != nil {
		log.Printf("ingestion publisher close failed: %v", err)
	}
	if err := redisClient.Close(); err != nil {
		log.Printf("ingestion redis client close failed: %v", err)
	}

	log.Print("ingestion service stopped")
}

func newPublisher(cfg config.Config) (publisher.Publisher, error) {
	switch cfg.PublisherBackend {
	case "", config.DefaultPublisherBackend:
		return publisher.NewLogPublisher(), nil
	case "kafka":
		log.Printf("using kafka publisher brokers=%v topic=%s", cfg.KafkaBrokers, publisher.TelemetryRawTopic)
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
