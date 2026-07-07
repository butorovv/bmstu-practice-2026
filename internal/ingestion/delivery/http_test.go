package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
)

type fakePublisher struct {
	err    error
	events []publisher.TelemetryEvent
}

func (f *fakePublisher) Publish(ctx context.Context, event publisher.TelemetryEvent) error {
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, event)
	return nil
}

func TestAcceptTelemetryReturnsAcceptedForValidRequest(t *testing.T) {
	pub := &fakePublisher{}
	router := NewRouter(NewHandler(pub, validator.New()))

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
}

func TestAcceptTelemetryReturnsBadRequestForInvalidJSON(t *testing.T) {
	pub := &fakePublisher{}
	router := NewRouter(NewHandler(pub, validator.New()))

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
}

func TestAcceptTelemetryReturnsServiceUnavailableForPublisherError(t *testing.T) {
	pub := &fakePublisher{err: errors.New("publish failed")}
	router := NewRouter(NewHandler(pub, validator.New()))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(validTelemetryJSON()))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	assertErrorCode(t, rec, "publisher_unavailable")
}

func TestHealthReturnsOK(t *testing.T) {
	router := NewRouter(NewHandler(&fakePublisher{}, validator.New()))

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
