package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	processinghttp "github.com/butorovv/bmstu-practice-2026/internal/processing/delivery/http"
	processingkafka "github.com/butorovv/bmstu-practice-2026/internal/processing/delivery/kafka"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/detector"
	postgresrepo "github.com/butorovv/bmstu-practice-2026/internal/processing/repository/postgres"
	redisrepo "github.com/butorovv/bmstu-practice-2026/internal/processing/repository/redis"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/shared/config"
)

const (
	retentionPeriod         = 30 * 24 * time.Hour
	retentionInterval       = 24 * time.Hour
	retentionCleanupTimeout = 30 * time.Second
)

type retentionCleaner interface {
	DeleteOlderThan(ctx context.Context, cutoff time.Time) error
}

func main() {
	cfg := config.LoadProcessing()

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelStartup()

	redisClient := redisrepo.NewClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err := redisClient.Ping(startupCtx).Err(); err != nil {
		log.Fatalf("failed to ping redis: %v", err)
	}

	postgresPool, err := postgresrepo.NewPool(startupCtx, cfg.Postgres)
	if err != nil {
		log.Fatalf("failed to connect postgres: %v", err)
	}
	if err := postgresrepo.Migrate(startupCtx, postgresPool); err != nil {
		postgresPool.Close()
		log.Fatalf("failed to migrate postgres: %v", err)
	}

	telemetryRepo := postgresrepo.NewTelemetryRepository(postgresPool)
	alertRepo := postgresrepo.NewAlertRepository(postgresPool)
	cleanupRepo := postgresrepo.NewCleanupRepository(postgresPool)
	windowRepo := redisrepo.NewSlidingWindowRepository(redisClient, cfg.WindowTTL)
	deduplicator := redisrepo.NewAlertDeduplicator(redisClient, cfg.AlertDedupTTL)
	service := usecase.NewProcessingService(
		telemetryRepo,
		alertRepo,
		detector.New(),
		windowRepo,
		deduplicator,
		cfg.AlertDedupTTL,
	)

	handler := processinghttp.NewHandler(telemetryRepo, alertRepo)
	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: processinghttp.NewRouter(handler),
	}
	consumer := processingkafka.NewConsumer(processingkafka.ConsumerConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.TelemetryTopic,
		DLQTopic: cfg.Kafka.DLQTopic,
		GroupID:  cfg.Kafka.ConsumerGroup,
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		runRetentionCleanup(ctx, cleanupRepo, log.Default())
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

	postgresPool.Close()
	if err := redisClient.Close(); err != nil {
		log.Printf("processing redis client close failed: %v", err)
	}

	log.Print("processing service stopped")
}

func runRetentionCleanup(ctx context.Context, cleaner retentionCleaner, logger *log.Logger) {
	runSingleRetentionCleanup(ctx, cleaner, logger)

	ticker := time.NewTicker(retentionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runSingleRetentionCleanup(ctx, cleaner, logger)
		}
	}
}

func runSingleRetentionCleanup(ctx context.Context, cleaner retentionCleaner, logger *log.Logger) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), retentionCleanupTimeout)
	defer cancel()

	cutoff := time.Now().UTC().Add(-retentionPeriod)
	if err := cleaner.DeleteOlderThan(cleanupCtx, cutoff); err != nil {
		logger.Printf("retention cleanup failed: %v", err)
		return
	}

	logger.Printf("retention cleanup completed cutoff=%s", cutoff.Format(time.RFC3339))
}
