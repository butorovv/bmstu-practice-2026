package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
)

type Handler struct {
	publisher    publisher.Publisher
	validator    validator.Validator
	idempotency  usecase.IdempotencyRepository
	rateLimiter  usecase.RateLimiter
	timeout      time.Duration
	readyTimeout time.Duration
	readyChecks  map[string]ReadinessCheck
	logger       *slog.Logger
	metrics      *Metrics
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

type readinessResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

const (
	idempotencyReleaseTimeout = time.Second
	defaultReadinessTimeout   = 2 * time.Second
	backpressureRetryAfter    = time.Second
)

type ReadinessCheck func(ctx context.Context) error

type HandlerOptions struct {
	RequestTimeout   time.Duration
	ReadinessTimeout time.Duration
	ReadinessChecks  map[string]ReadinessCheck
	Logger           *slog.Logger
	Metrics          *Metrics
}

func NewHandler(
	pub publisher.Publisher,
	val validator.Validator,
	idempotency usecase.IdempotencyRepository,
	rateLimiter usecase.RateLimiter,
) *Handler {
	return NewHandlerWithOptions(pub, val, idempotency, rateLimiter, HandlerOptions{})
}

func NewHandlerWithOptions(
	pub publisher.Publisher,
	val validator.Validator,
	idempotency usecase.IdempotencyRepository,
	rateLimiter usecase.RateLimiter,
	opts HandlerOptions,
) *Handler {
	readinessTimeout := opts.ReadinessTimeout
	if readinessTimeout <= 0 {
		readinessTimeout = defaultReadinessTimeout
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Handler{
		publisher:    pub,
		validator:    val,
		idempotency:  idempotency,
		rateLimiter:  rateLimiter,
		timeout:      opts.RequestTimeout,
		readyTimeout: readinessTimeout,
		readyChecks:  opts.ReadinessChecks,
		logger:       logger,
		metrics:      opts.Metrics,
	}
}

func NewRouter(handler *Handler, metricsHandler ...http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/telemetry", handler.AcceptTelemetry)
	mux.HandleFunc("GET /health", handler.Health)
	mux.HandleFunc("GET /ready", handler.Ready)
	if len(metricsHandler) > 0 && metricsHandler[0] != nil {
		mux.Handle("GET /metrics", metricsHandler[0])
	}
	return observeHTTP(mux, handler.logger, handler.metrics)
}

func (h *Handler) AcceptTelemetry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if h.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.timeout)
		defer cancel()
	}

	var batch usecase.TelemetryBatch

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil {
		h.logger.WarnContext(ctx, "telemetry request contains invalid JSON", "error", err)
		writeError(w, http.StatusBadRequest, "invalid_batch", "invalid JSON body")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		h.logger.WarnContext(ctx, "telemetry request contains trailing JSON data")
		writeError(w, http.StatusBadRequest, "invalid_batch", "invalid JSON body")
		return
	}

	if err := h.validator.ValidateBatch(batch); err != nil {
		h.logger.WarnContext(
			ctx,
			"telemetry batch validation failed",
			"device_id", batch.DeviceID,
			"batch_id", batch.BatchID,
			"error", err,
		)
		writeError(w, http.StatusBadRequest, "invalid_batch", err.Error())
		return
	}

	reserved, err := h.idempotency.Reserve(ctx, batch.DeviceID, batch.BatchID)
	if err != nil {
		h.logger.ErrorContext(
			ctx,
			"idempotency reservation failed",
			"device_id", batch.DeviceID,
			"batch_id", batch.BatchID,
			"error", err,
		)
		writeError(
			w,
			http.StatusServiceUnavailable,
			"infrastructure_unavailable",
			"required infrastructure is unavailable",
		)
		return
	}
	if !reserved {
		h.logger.InfoContext(
			ctx,
			"duplicate telemetry batch ignored",
			"device_id", batch.DeviceID,
			"batch_id", batch.BatchID,
		)
		writeJSON(w, http.StatusOK, duplicateIgnoredResponse{Status: "duplicate_ignored"})
		return
	}

	rateLimit, err := h.rateLimiter.Allow(ctx, batch.DeviceID)
	if err != nil {
		_ = h.releaseIdempotency(ctx, batch.DeviceID, batch.BatchID)
		h.logger.ErrorContext(
			ctx,
			"rate limit check failed",
			"device_id", batch.DeviceID,
			"batch_id", batch.BatchID,
			"error", err,
		)
		writeError(
			w,
			http.StatusServiceUnavailable,
			"infrastructure_unavailable",
			"required infrastructure is unavailable",
		)
		return
	}
	if !rateLimit.Allowed {
		if err := h.releaseIdempotency(ctx, batch.DeviceID, batch.BatchID); err != nil {
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
		h.logger.WarnContext(
			ctx,
			"telemetry batch rate limited",
			"device_id", batch.DeviceID,
			"batch_id", batch.BatchID,
			"retry_after_seconds", retryAfter,
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
		if err := h.publisher.Publish(ctx, event); err != nil {
			h.metrics.observePublishError()
			_ = h.releaseIdempotency(ctx, batch.DeviceID, batch.BatchID)
			if errors.Is(err, publisher.ErrBackpressure) {
				h.metrics.observeBackpressure()
				retryAfter := int64(backpressureRetryAfter / time.Second)
				w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
				h.logger.WarnContext(
					ctx,
					"telemetry batch rejected by publisher backpressure",
					"device_id", batch.DeviceID,
					"batch_id", batch.BatchID,
					"retry_after_seconds", retryAfter,
				)
				writeError(
					w,
					http.StatusTooManyRequests,
					"publisher_backpressure",
					"publisher is busy; retry later",
				)
				return
			}
			h.logger.ErrorContext(
				ctx,
				"telemetry event publication failed",
				"device_id", batch.DeviceID,
				"batch_id", batch.BatchID,
				"event_id", event.EventID,
				"error", err,
			)
			writeError(w, http.StatusServiceUnavailable, "publisher_unavailable", "publisher is unavailable")
			return
		}
		h.metrics.observePublishedEvent()
	}

	h.logger.InfoContext(
		ctx,
		"telemetry batch accepted",
		"device_id", batch.DeviceID,
		"batch_id", batch.BatchID,
		"measurements", len(events),
	)

	writeJSON(w, http.StatusAccepted, acceptedResponse{
		Status:               "accepted",
		AcceptedMeasurements: len(events),
	})
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	checks := make(map[string]string, len(h.readyChecks))
	if len(h.readyChecks) == 0 {
		writeJSON(w, http.StatusOK, readinessResponse{Status: "ready", Checks: checks})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.readyTimeout)
	defer cancel()

	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(h.readyChecks))
	pending := make(map[string]struct{}, len(h.readyChecks))
	for name, check := range h.readyChecks {
		pending[name] = struct{}{}
		go func() {
			results <- result{name: name, err: check(ctx)}
		}()
	}

	ready := true
	for len(pending) > 0 {
		select {
		case checkResult := <-results:
			delete(pending, checkResult.name)
			if checkResult.err == nil {
				checks[checkResult.name] = "ok"
				h.metrics.setReadiness(checkResult.name, true)
				continue
			}

			ready = false
			checks[checkResult.name] = "unavailable"
			h.metrics.setReadiness(checkResult.name, false)
			h.logger.WarnContext(
				r.Context(),
				"readiness check failed",
				"dependency", checkResult.name,
				"error", checkResult.err,
			)
		case <-ctx.Done():
			ready = false
			for name := range pending {
				checks[name] = "unavailable"
				h.metrics.setReadiness(name, false)
				h.logger.WarnContext(
					r.Context(),
					"readiness check timed out",
					"dependency", name,
					"error", ctx.Err(),
				)
			}
			pending = nil
		}
	}

	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, readinessResponse{
			Status: "not_ready",
			Checks: checks,
		})
		return
	}

	writeJSON(w, http.StatusOK, readinessResponse{Status: "ready", Checks: checks})
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
