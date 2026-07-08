package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	processinghttp "github.com/butorovv/bmstu-practice-2026/internal/processing/delivery/http"
	"github.com/butorovv/bmstu-practice-2026/internal/shared/config"
)

func main() {
	cfg := config.LoadProcessing()

	handler := processinghttp.NewHandler()
	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: processinghttp.NewRouter(handler),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("starting processing service on %s", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("processing service stopped: %v", err)
		}
	}()

	<-ctx.Done()
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("processing service shutdown failed: %v", err)
	}

	log.Print("processing service stopped")
}
