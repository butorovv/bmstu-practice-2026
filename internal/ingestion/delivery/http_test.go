package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
	"github.com/prometheus/client_golang/prometheus"
)

type fakePublisher struct {
	err            error
	waitForContext bool
	events         []publisher.TelemetryEvent
}

func (f *fakePublisher) Publish(ctx context.Context, event publisher.TelemetryEvent) error {
	if f.waitForContext {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, event)
	return nil
}

type fakeIdempotencyRepository struct {
	reserved     bool
	err          error
	releaseErr   error
	reserveCalls int
	releaseCalls int
	deviceID     string
	batchID      string
}

var _ usecase.IdempotencyRepository = (*fakeIdempotencyRepository)(nil)

func (f *fakeIdempotencyRepository) Reserve(
	_ context.Context,
	deviceID string,
	batchID string,
) (bool, error) {
	f.reserveCalls++
	f.deviceID = deviceID
	f.batchID = batchID
	return f.reserved, f.err
}

func (f *fakeIdempotencyRepository) Release(
	_ context.Context,
	deviceID string,
	batchID string,
) error {
	f.releaseCalls++
	f.deviceID = deviceID
	f.batchID = batchID
	return f.releaseErr
}

type fakeRateLimiter struct {
	decision usecase.RateLimitDecision
	err      error
	calls    int
	deviceID string
}

var _ usecase.RateLimiter = (*fakeRateLimiter)(nil)

func (f *fakeRateLimiter) Allow(
	_ context.Context,
	deviceID string,
) (usecase.RateLimitDecision, error) {
	f.calls++
	f.deviceID = deviceID
	return f.decision, f.err
}

func allowingRateLimiter() *fakeRateLimiter {
	return &fakeRateLimiter{
		decision: usecase.RateLimitDecision{Allowed: true},
	}
}

func TestAcceptTelemetryReturnsAcceptedForValidRequest(t *testing.T) {
	pub := &fakePublisher{}
	idempotency := &fakeIdempotencyRepository{reserved: true}
	rateLimiter := allowingRateLimiter()
	router := NewRouter(NewHandler(pub, validator.New(), idempotency, rateLimiter))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(validTelemetryJSON()))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}

	var response acceptedResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "accepted" {
		t.Fatalf("response status = %q, want %q", response.Status, "accepted")
	}
	if response.AcceptedMeasurements != 1 {
		t.Fatalf("accepted_measurements = %d, want 1", response.AcceptedMeasurements)
	}
	if len(pub.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(pub.events))
	}
	if pub.events[0].EventID != "device-001-000001-0" {
		t.Fatalf("event_id = %q, want %q", pub.events[0].EventID, "device-001-000001-0")
	}
	if idempotency.reserveCalls != 1 {
		t.Fatalf("idempotency reserve calls = %d, want 1", idempotency.reserveCalls)
	}
	if idempotency.deviceID != "device-001" || idempotency.batchID != "device-001-000001" {
		t.Fatalf("idempotency key parts = %q, %q", idempotency.deviceID, idempotency.batchID)
	}
	if rateLimiter.calls != 1 || rateLimiter.deviceID != "device-001" {
		t.Fatalf("rate limiter calls = %d, device = %q", rateLimiter.calls, rateLimiter.deviceID)
	}
}

func TestAcceptTelemetryReturnsBadRequestForInvalidJSON(t *testing.T) {
	pub := &fakePublisher{}
	idempotency := &fakeIdempotencyRepository{reserved: true}
	rateLimiter := allowingRateLimiter()
	router := NewRouter(NewHandler(pub, validator.New(), idempotency, rateLimiter))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(`{"device_id":`))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "invalid_batch")
	if len(pub.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(pub.events))
	}
	if idempotency.reserveCalls != 0 {
		t.Fatalf("idempotency reserve calls = %d, want 0", idempotency.reserveCalls)
	}
	if rateLimiter.calls != 0 {
		t.Fatalf("rate limiter calls = %d, want 0", rateLimiter.calls)
	}
}

func TestAcceptTelemetryReturnsServiceUnavailableForPublisherError(t *testing.T) {
	pub := &fakePublisher{err: errors.New("publish failed")}
	idempotency := &fakeIdempotencyRepository{reserved: true}
	router := NewRouter(NewHandler(pub, validator.New(), idempotency, allowingRateLimiter()))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(validTelemetryJSON()))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	assertErrorCode(t, rec, "publisher_unavailable")
	if idempotency.releaseCalls != 1 {
		t.Fatalf("idempotency release calls = %d, want 1", idempotency.releaseCalls)
	}
}

func TestAcceptTelemetryReturnsTooManyRequestsForPublisherBackpressure(t *testing.T) {
	pub := &fakePublisher{err: publisher.ErrBackpressure}
	idempotency := &fakeIdempotencyRepository{reserved: true}
	router := NewRouter(NewHandler(pub, validator.New(), idempotency, allowingRateLimiter()))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(validTelemetryJSON()))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if retryAfter := rec.Header().Get("Retry-After"); retryAfter != "1" {
		t.Fatalf("Retry-After = %q, want 1", retryAfter)
	}
	assertErrorCode(t, rec, "publisher_backpressure")
	if idempotency.releaseCalls != 1 {
		t.Fatalf("idempotency release calls = %d, want 1", idempotency.releaseCalls)
	}
}

func TestAcceptTelemetryReturnsServiceUnavailableWhenRequestTimesOut(t *testing.T) {
	pub := &fakePublisher{waitForContext: true}
	idempotency := &fakeIdempotencyRepository{reserved: true}
	handler := NewHandlerWithOptions(
		pub,
		validator.New(),
		idempotency,
		allowingRateLimiter(),
		HandlerOptions{RequestTimeout: time.Millisecond},
	)
	router := NewRouter(handler)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(validTelemetryJSON()))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	assertErrorCode(t, rec, "publisher_unavailable")
	if idempotency.releaseCalls != 1 {
		t.Fatalf("idempotency release calls = %d, want 1", idempotency.releaseCalls)
	}
}

func TestAcceptTelemetryReturnsDuplicateIgnoredWithoutPublishing(t *testing.T) {
	pub := &fakePublisher{}
	idempotency := &fakeIdempotencyRepository{reserved: false}
	rateLimiter := allowingRateLimiter()
	router := NewRouter(NewHandler(pub, validator.New(), idempotency, rateLimiter))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(validTelemetryJSON()))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var response duplicateIgnoredResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "duplicate_ignored" {
		t.Fatalf("response status = %q, want %q", response.Status, "duplicate_ignored")
	}
	if len(pub.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(pub.events))
	}
	if rateLimiter.calls != 0 {
		t.Fatalf("rate limiter calls = %d, want 0", rateLimiter.calls)
	}
}

func TestAcceptTelemetryReturnsServiceUnavailableForIdempotencyError(t *testing.T) {
	pub := &fakePublisher{}
	idempotency := &fakeIdempotencyRepository{err: errors.New("Redis unavailable")}
	rateLimiter := allowingRateLimiter()
	router := NewRouter(NewHandler(pub, validator.New(), idempotency, rateLimiter))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(validTelemetryJSON()))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	assertErrorCode(t, rec, "infrastructure_unavailable")
	if len(pub.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(pub.events))
	}
	if rateLimiter.calls != 0 {
		t.Fatalf("rate limiter calls = %d, want 0", rateLimiter.calls)
	}
}

func TestAcceptTelemetryReturnsTooManyRequestsWithRetryAfter(t *testing.T) {
	pub := &fakePublisher{}
	idempotency := &fakeIdempotencyRepository{reserved: true}
	rateLimiter := &fakeRateLimiter{
		decision: usecase.RateLimitDecision{
			Allowed:    false,
			RetryAfter: 4 * time.Second,
		},
	}
	router := NewRouter(NewHandler(pub, validator.New(), idempotency, rateLimiter))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(validTelemetryJSON()))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if retryAfter := rec.Header().Get("Retry-After"); retryAfter != "4" {
		t.Fatalf("Retry-After = %q, want %q", retryAfter, "4")
	}
	assertErrorCode(t, rec, "rate_limit_exceeded")
	if len(pub.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(pub.events))
	}
	if idempotency.releaseCalls != 1 {
		t.Fatalf("idempotency release calls = %d, want 1", idempotency.releaseCalls)
	}
}

func TestAcceptTelemetryReturnsServiceUnavailableForRateLimiterError(t *testing.T) {
	pub := &fakePublisher{}
	idempotency := &fakeIdempotencyRepository{reserved: true}
	rateLimiter := &fakeRateLimiter{err: errors.New("Redis unavailable")}
	router := NewRouter(NewHandler(pub, validator.New(), idempotency, rateLimiter))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(validTelemetryJSON()))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	assertErrorCode(t, rec, "infrastructure_unavailable")
	if len(pub.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(pub.events))
	}
	if idempotency.releaseCalls != 1 {
		t.Fatalf("idempotency release calls = %d, want 1", idempotency.releaseCalls)
	}
}

func TestHealthReturnsOK(t *testing.T) {
	router := NewRouter(NewHandler(
		&fakePublisher{},
		validator.New(),
		&fakeIdempotencyRepository{reserved: true},
		allowingRateLimiter(),
	))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var response healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "ok" {
		t.Fatalf("response status = %q, want %q", response.Status, "ok")
	}
}

func TestReadyReturnsOKWhenDependenciesAreAvailable(t *testing.T) {
	handler := NewHandlerWithOptions(
		&fakePublisher{},
		validator.New(),
		&fakeIdempotencyRepository{reserved: true},
		allowingRateLimiter(),
		HandlerOptions{ReadinessChecks: map[string]ReadinessCheck{
			"kafka": func(context.Context) error { return nil },
			"redis": func(context.Context) error { return nil },
		}},
	)
	router := NewRouter(handler)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var response readinessResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "ready" {
		t.Fatalf("response status = %q, want ready", response.Status)
	}
	if response.Checks["kafka"] != "ok" || response.Checks["redis"] != "ok" {
		t.Fatalf("readiness checks = %v", response.Checks)
	}
}

func TestReadyReturnsServiceUnavailableWhenDependencyIsUnavailable(t *testing.T) {
	handler := NewHandlerWithOptions(
		&fakePublisher{},
		validator.New(),
		&fakeIdempotencyRepository{reserved: true},
		allowingRateLimiter(),
		HandlerOptions{ReadinessChecks: map[string]ReadinessCheck{
			"kafka": func(context.Context) error { return errors.New("Kafka unavailable") },
			"redis": func(context.Context) error { return nil },
		}},
	)
	router := NewRouter(handler)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	var response readinessResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "not_ready" {
		t.Fatalf("response status = %q, want not_ready", response.Status)
	}
	if response.Checks["kafka"] != "unavailable" || response.Checks["redis"] != "ok" {
		t.Fatalf("readiness checks = %v", response.Checks)
	}
}

func TestReadyReturnsServiceUnavailableWhenCheckTimesOut(t *testing.T) {
	blocked := make(chan struct{})
	defer close(blocked)

	handler := NewHandlerWithOptions(
		&fakePublisher{},
		validator.New(),
		&fakeIdempotencyRepository{reserved: true},
		allowingRateLimiter(),
		HandlerOptions{
			ReadinessTimeout: time.Millisecond,
			ReadinessChecks: map[string]ReadinessCheck{
				"kafka": func(context.Context) error {
					<-blocked
					return nil
				},
			},
		},
	)
	router := NewRouter(handler)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	var response readinessResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Checks["kafka"] != "unavailable" {
		t.Fatalf("readiness checks = %v", response.Checks)
	}
}

func TestMetricsExposeHTTPAndPublishingObservations(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	handler := NewHandlerWithOptions(
		&fakePublisher{},
		validator.New(),
		&fakeIdempotencyRepository{reserved: true},
		allowingRateLimiter(),
		HandlerOptions{Metrics: metrics},
	)
	router := NewRouter(handler, MetricsHandler(registry))

	telemetryRequest := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/telemetry",
		bytes.NewBufferString(validTelemetryJSON()),
	)
	telemetryResponse := httptest.NewRecorder()
	router.ServeHTTP(telemetryResponse, telemetryRequest)
	if telemetryResponse.Code != http.StatusAccepted {
		t.Fatalf("telemetry status = %d, want %d", telemetryResponse.Code, http.StatusAccepted)
	}

	metricsRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsResponse := httptest.NewRecorder()
	router.ServeHTTP(metricsResponse, metricsRequest)

	if metricsResponse.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", metricsResponse.Code, http.StatusOK)
	}
	body := metricsResponse.Body.String()
	for _, want := range []string{
		`ingestion_http_requests_total{method="POST",route="POST /api/v1/telemetry",status="202"} 1`,
		`ingestion_telemetry_events_published_total 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body does not contain %q\n%s", want, body)
		}
	}
}

func TestRouterWritesStructuredRequestLog(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	handler := NewHandlerWithOptions(
		&fakePublisher{},
		validator.New(),
		&fakeIdempotencyRepository{reserved: true},
		allowingRateLimiter(),
		HandlerOptions{Logger: logger},
	)
	router := NewRouter(handler)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	logLine := output.String()
	for _, want := range []string{
		`"msg":"http request completed"`,
		`"method":"GET"`,
		`"route":"GET /health"`,
		`"status":200`,
	} {
		if !strings.Contains(logLine, want) {
			t.Fatalf("structured log does not contain %q: %s", want, logLine)
		}
	}
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()

	var response errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error != want {
		t.Fatalf("error code = %q, want %q", response.Error, want)
	}
}

func validTelemetryJSON() string {
	return `{
		"device_id": "device-001",
		"patient_id": "patient-001",
		"batch_id": "device-001-000001",
		"measurements": [
			{
				"timestamp": "2026-07-07T12:00:00Z",
				"heart_rate": 78
			}
		]
	}`
}
