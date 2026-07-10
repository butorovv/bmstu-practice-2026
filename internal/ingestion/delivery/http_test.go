package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
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
