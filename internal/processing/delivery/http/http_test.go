package http

import (
	"context"
	"encoding/json"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/storage"
)

func TestHealthReturnsOK(t *testing.T) {
	router := NewRouter(NewHandler(storage.NewInMemoryAlertRepository()))

	req := httptest.NewRequest(nethttp.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != nethttp.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, nethttp.StatusOK)
	}

	var response healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "ok" {
		t.Fatalf("response status = %q, want %q", response.Status, "ok")
	}
	if response.Service != "processing" {
		t.Fatalf("response service = %q, want %q", response.Service, "processing")
	}
}

func TestRouterDoesNotExposeTelemetryPost(t *testing.T) {
	router := NewRouter(NewHandler(storage.NewInMemoryAlertRepository()))

	req := httptest.NewRequest(nethttp.MethodPost, "/api/v1/telemetry", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code == nethttp.StatusOK || rec.Code == nethttp.StatusAccepted {
		t.Fatalf("POST /api/v1/telemetry status = %d, want non-success", rec.Code)
	}
}

func TestGetAlertsReturnsAllAlerts(t *testing.T) {
	alertRepo := storage.NewInMemoryAlertRepository()
	mustSaveAlert(t, alertRepo, testAlert(7, "patient-001"))
	mustSaveAlert(t, alertRepo, testAlert(8, "patient-002"))
	router := NewRouter(NewHandler(alertRepo))

	req := httptest.NewRequest(nethttp.MethodGet, "/alerts", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != nethttp.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, nethttp.StatusOK)
	}

	var response []model.Alert
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response) != 2 {
		t.Fatalf("alerts count = %d, want 2", len(response))
	}
	if response[0].ID != 7 {
		t.Fatalf("first alert id = %d, want 7", response[0].ID)
	}
	if response[1].ID != 8 {
		t.Fatalf("second alert id = %d, want 8", response[1].ID)
	}
}

func TestGetAlertsReturnsEmptyArray(t *testing.T) {
	router := NewRouter(NewHandler(storage.NewInMemoryAlertRepository()))

	req := httptest.NewRequest(nethttp.MethodGet, "/alerts", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != nethttp.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, nethttp.StatusOK)
	}
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("body = %q, want []", rec.Body.String())
	}
}

func TestGetAlertsByPatientID(t *testing.T) {
	alertRepo := storage.NewInMemoryAlertRepository()
	mustSaveAlert(t, alertRepo, testAlert(7, "patient-001"))
	mustSaveAlert(t, alertRepo, testAlert(8, "patient-002"))
	mustSaveAlert(t, alertRepo, testAlert(9, "patient-001"))
	router := NewRouter(NewHandler(alertRepo))

	req := httptest.NewRequest(nethttp.MethodGet, "/alerts/patient-001", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != nethttp.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, nethttp.StatusOK)
	}

	var response []model.Alert
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response) != 2 {
		t.Fatalf("alerts count = %d, want 2", len(response))
	}
	for _, alert := range response {
		if alert.PatientID != "patient-001" {
			t.Fatalf("alert patient_id = %q, want patient-001", alert.PatientID)
		}
	}
}

func TestGetAlertsByPatientIDReturnsEmptyArray(t *testing.T) {
	router := NewRouter(NewHandler(storage.NewInMemoryAlertRepository()))

	req := httptest.NewRequest(nethttp.MethodGet, "/alerts/patient-001", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != nethttp.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, nethttp.StatusOK)
	}
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("body = %q, want []", rec.Body.String())
	}
}

func mustSaveAlert(t *testing.T, repo *storage.InMemoryAlertRepository, alert model.Alert) {
	t.Helper()

	if err := repo.SaveAlert(context.Background(), alert); err != nil {
		t.Fatalf("SaveAlert() error = %v", err)
	}
}

func testAlert(id int64, patientID string) model.Alert {
	return model.Alert{
		ID:          id,
		PatientID:   patientID,
		Type:        model.AlertTypeHighHeartRate,
		Message:     model.HighHeartRateMessage,
		TriggeredAt: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC),
	}
}
