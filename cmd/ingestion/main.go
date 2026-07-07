package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/delivery"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
	"github.com/butorovv/bmstu-practice-2026/internal/shared/config"
)

func main() {
	cfg := config.Load()

	handler := delivery.NewHandler(
		newPublisher(cfg),
		validator.New(),
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

func newPublisher(cfg config.Config) publisher.Publisher {
	switch cfg.PublisherBackend {
	case "", config.DefaultPublisherBackend:
		return publisher.NewLogPublisher()
	default:
		log.Printf("unknown PUBLISHER_BACKEND=%q, falling back to log publisher", cfg.PublisherBackend)
		return publisher.NewLogPublisher()
	}
}
