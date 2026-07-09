package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
)

type Handler struct {
	publisher   publisher.Publisher
	validator   validator.Validator
	idempotency usecase.IdempotencyRepository
	rateLimiter usecase.RateLimiter
}

type acceptedResponse struct {
	Status               string `json:"status"`
	AcceptedMeasurements int    `json:"accepted_measurements"`
}

type duplicateIgnoredResponse struct {
	Status string `json:"status"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type healthResponse struct {
	Status string `json:"status"`
}

const idempotencyReleaseTimeout = time.Second

func NewHandler(
	pub publisher.Publisher,
	val validator.Validator,
	idempotency usecase.IdempotencyRepository,
	rateLimiter usecase.RateLimiter,
) *Handler {
	return &Handler{
		publisher:   pub,
		validator:   val,
		idempotency: idempotency,
		rateLimiter: rateLimiter,
	}
}

func NewRouter(handler *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/telemetry", handler.AcceptTelemetry)
	mux.HandleFunc("GET /health", handler.Health)
	return mux
}

func (h *Handler) AcceptTelemetry(w http.ResponseWriter, r *http.Request) {
	var batch usecase.TelemetryBatch

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_batch", "invalid JSON body")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_batch", "invalid JSON body")
		return
	}

	if err := h.validator.ValidateBatch(batch); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_batch", err.Error())
		return
	}

	reserved, err := h.idempotency.Reserve(r.Context(), batch.DeviceID, batch.BatchID)
	if err != nil {
		writeError(
			w,
			http.StatusServiceUnavailable,
			"infrastructure_unavailable",
			"required infrastructure is unavailable",
		)
		return
	}
	if !reserved {
		writeJSON(w, http.StatusOK, duplicateIgnoredResponse{Status: "duplicate_ignored"})
		return
	}

	rateLimit, err := h.rateLimiter.Allow(r.Context(), batch.DeviceID)
	if err != nil {
		_ = h.releaseIdempotency(r.Context(), batch.DeviceID, batch.BatchID)
		writeError(
			w,
			http.StatusServiceUnavailable,
			"infrastructure_unavailable",
			"required infrastructure is unavailable",
		)
		return
	}
	if !rateLimit.Allowed {
		if err := h.releaseIdempotency(r.Context(), batch.DeviceID, batch.BatchID); err != nil {
			writeError(
				w,
				http.StatusServiceUnavailable,
				"infrastructure_unavailable",
				"required infrastructure is unavailable",
			)
			return
		}

		retryAfter := max(
			int64((rateLimit.RetryAfter+time.Second-1)/time.Second),
			1,
		)
		w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
		writeError(
			w,
			http.StatusTooManyRequests,
			"rate_limit_exceeded",
			"device rate limit exceeded",
		)
		return
	}

	events := usecase.BuildEvents(batch)
	for _, event := range events {
		if err := h.publisher.Publish(r.Context(), event); err != nil {
			_ = h.releaseIdempotency(r.Context(), batch.DeviceID, batch.BatchID)
			writeError(w, http.StatusServiceUnavailable, "publisher_unavailable", "publisher is unavailable")
			return
		}
	}

	writeJSON(w, http.StatusAccepted, acceptedResponse{
		Status:               "accepted",
		AcceptedMeasurements: len(events),
	})
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (h *Handler) releaseIdempotency(
	ctx context.Context,
	deviceID string,
	batchID string,
) error {
	releaseCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		idempotencyReleaseTimeout,
	)
	defer cancel()

	return h.idempotency.Release(releaseCtx, deviceID, batchID)
}

func writeError(w http.ResponseWriter, statusCode int, code string, message string) {
	writeJSON(w, statusCode, errorResponse{
		Error:   code,
		Message: message,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
