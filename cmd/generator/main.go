package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/butorovv/bmstu-practice-2026/internal/generator"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := generator.LoadConfig(os.Args[1:])
	if err != nil {
		logger.Error("invalid generator configuration", "error", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if _, err := generator.New(cfg, logger).Run(ctx); err != nil {
		logger.Error("load generator stopped with error", "error", err)
		os.Exit(1)
	}
}
