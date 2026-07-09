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
	redisrepository "github.com/butorovv/bmstu-practice-2026/internal/ingestion/repository/redis"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
	"github.com/butorovv/bmstu-practice-2026/internal/shared/config"
)

func main() {
	cfg := config.Load()

	pub, err := newPublisher(cfg)
	if err != nil {
		log.Fatalf("configure publisher: %v", err)
	}
	if closer, ok := pub.(interface{ Close() error }); ok {
		defer func() {
			if err := closer.Close(); err != nil {
				log.Printf("close publisher: %v", err)
			}
		}()
	}

	redisClient := redisrepository.NewClient(
		cfg.RedisAddr,
		cfg.RedisPassword,
		cfg.RedisDB,
	)
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("close Redis: %v", err)
		}
	}()
	idempotencyRepository := redisrepository.NewIdempotencyRepository(redisClient)
	rateLimiter := redisrepository.NewRateLimiter(redisClient)

	handler := delivery.NewHandler(
		pub,
		validator.New(),
		idempotencyRepository,
		rateLimiter,
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

	log.Print("ingestion service stopped")
}

func newPublisher(cfg config.Config) (publisher.Publisher, error) {
	switch cfg.PublisherBackend {
	case "", config.DefaultPublisherBackend:
		return publisher.NewLogPublisher(), nil
	case "kafka":
		return publisher.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaPublishTimeout)
	default:
		return nil, fmt.Errorf("unsupported PUBLISHER_BACKEND %q", cfg.PublisherBackend)
	}
}
