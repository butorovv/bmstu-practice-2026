package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"

	processinghttp "github.com/butorovv/bmstu-practice-2026/internal/processing/delivery/http"
	processingkafka "github.com/butorovv/bmstu-practice-2026/internal/processing/delivery/kafka"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/detector"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/storage"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/shared/config"
)

func main() {
	cfg := config.LoadProcessing()

	telemetryRepo := storage.NewInMemoryTelemetryRepository()
	alertRepo := storage.NewInMemoryAlertRepository()
	service := usecase.NewProcessingService(telemetryRepo, alertRepo, detector.New())

	handler := processinghttp.NewHandler(alertRepo)
	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: processinghttp.NewRouter(handler),
	}
	consumer := processingkafka.NewConsumer(processingkafka.ConsumerConfig{
		Brokers: cfg.Kafka.Brokers,
		Topic:   cfg.Kafka.TelemetryTopic,
		GroupID: cfg.Kafka.ConsumerGroup,
	}, processingkafka.NewMessageHandler(service), log.Default())

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("starting processing service on %s", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("http server stopped: %w", err)
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Run(ctx); err != nil {
			errCh <- fmt.Errorf("kafka consumer stopped: %w", err)
			cancel()
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		log.Printf("processing service runtime error: %v", err)
		cancel()
	}
	stopSignals()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("processing service shutdown failed: %v", err)
	}
	if err := consumer.Close(); err != nil {
		log.Printf("processing kafka consumer close failed: %v", err)
	}
	wg.Wait()

	log.Print("processing service stopped")
}
