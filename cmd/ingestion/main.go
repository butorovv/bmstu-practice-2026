package main

import (
	"log"
	"net/http"
	"os"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/delivery"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
)

func main() {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	handler := delivery.NewHandler(
		publisher.NewLogPublisher(),
		validator.New(),
	)

	log.Printf("starting ingestion service on %s", addr)
	if err := http.ListenAndServe(addr, delivery.NewRouter(handler)); err != nil {
		log.Fatalf("ingestion service stopped: %v", err)
	}
}
