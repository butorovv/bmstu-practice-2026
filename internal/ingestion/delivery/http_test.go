package delivery

import (
	"bytes"
	"context"
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
