package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
)

var (
	_ usecase.TelemetryRepository = (*InMemoryTelemetryRepository)(nil)
	_ usecase.AlertRepository     = (*InMemoryAlertRepository)(nil)
)

func TestInMemoryTelemetryRepository(t *testing.T) {
	repo := NewInMemoryTelemetryRepository()
	event := storageTelemetryEvent("event-001", "patient-001")

	if err := repo.SaveTelemetry(context.Background(), event); err != nil {
		t.Fatalf("SaveTelemetry() error = %v", err)
	}

	events, err := repo.GetTelemetry(context.Background())
	if err != nil {
		t.Fatalf("GetTelemetry() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events count = %d, want 1", len(events))
	}
	if events[0].EventID != event.EventID {
		t.Fatalf("event_id = %q, want %q", events[0].EventID, event.EventID)
	}

	events[0].EventID = "changed"
	events, err = repo.GetTelemetry(context.Background())
	if err != nil {
		t.Fatalf("GetTelemetry() error = %v", err)
	}
	if events[0].EventID != event.EventID {
		t.Fatalf("repository returned mutable telemetry slice")
	}
}

func TestInMemoryAlertRepository(t *testing.T) {
	repo := NewInMemoryAlertRepository()
	alerts := []model.Alert{
		storageAlert(0, "patient-001"),
		storageAlert(0, "patient-002"),
		storageAlert(10, "patient-001"),
	}

	for _, alert := range alerts {
		if err := repo.SaveAlert(context.Background(), alert); err != nil {
			t.Fatalf("SaveAlert() error = %v", err)
		}
	}

	recentAlerts, err := repo.GetRecentAlerts(context.Background())
	if err != nil {
		t.Fatalf("GetRecentAlerts() error = %v", err)
	}
	if len(recentAlerts) != 3 {
		t.Fatalf("recent alerts count = %d, want 3", len(recentAlerts))
	}
	if recentAlerts[0].ID != 1 {
		t.Fatalf("first alert id = %d, want 1", recentAlerts[0].ID)
	}
	if recentAlerts[1].ID != 2 {
		t.Fatalf("second alert id = %d, want 2", recentAlerts[1].ID)
	}
	if recentAlerts[2].ID != 10 {
		t.Fatalf("third alert id = %d, want 10", recentAlerts[2].ID)
	}

	patientAlerts, err := repo.GetAlertsByPatientID(context.Background(), "patient-001")
	if err != nil {
		t.Fatalf("GetAlertsByPatientID() error = %v", err)
	}
	if len(patientAlerts) != 2 {
		t.Fatalf("patient alerts count = %d, want 2", len(patientAlerts))
	}

	missingAlerts, err := repo.GetAlertsByPatientID(context.Background(), "missing")
	if err != nil {
		t.Fatalf("GetAlertsByPatientID() error = %v", err)
	}
	if len(missingAlerts) != 0 {
		t.Fatalf("missing patient alerts count = %d, want 0", len(missingAlerts))
	}

	recentAlerts[0].PatientID = "changed"
	recentAlerts, err = repo.GetRecentAlerts(context.Background())
	if err != nil {
		t.Fatalf("GetRecentAlerts() error = %v", err)
	}
	if recentAlerts[0].PatientID != "patient-001" {
		t.Fatalf("repository returned mutable alerts slice")
	}
}

func TestInMemoryRepositoriesConcurrentAccess(t *testing.T) {
	telemetryRepo := NewInMemoryTelemetryRepository()
	alertRepo := NewInMemoryAlertRepository()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := telemetryRepo.SaveTelemetry(context.Background(), storageTelemetryEvent(fmt.Sprintf("event-%03d", i), "patient-001")); err != nil {
				t.Errorf("SaveTelemetry() error = %v", err)
			}
			if err := alertRepo.SaveAlert(context.Background(), storageAlert(0, "patient-001")); err != nil {
				t.Errorf("SaveAlert() error = %v", err)
			}
		}()
	}
	wg.Wait()

	events, err := telemetryRepo.GetTelemetry(context.Background())
	if err != nil {
		t.Fatalf("GetTelemetry() error = %v", err)
	}
	if len(events) != 100 {
		t.Fatalf("events count = %d, want 100", len(events))
	}

	alerts, err := alertRepo.GetRecentAlerts(context.Background())
	if err != nil {
		t.Fatalf("GetRecentAlerts() error = %v", err)
	}
	if len(alerts) != 100 {
		t.Fatalf("alerts count = %d, want 100", len(alerts))
	}
}

func storageTelemetryEvent(eventID string, patientID string) model.TelemetryEvent {
	return model.TelemetryEvent{
		EventID:   eventID,
		DeviceID:  "device-001",
		PatientID: patientID,
		Timestamp: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC),
		HeartRate: 78,
	}
}

func storageAlert(id int64, patientID string) model.Alert {
	return model.Alert{
		ID:          id,
		PatientID:   patientID,
		Type:        model.AlertTypeHighHeartRate,
		Message:     model.HighHeartRateMessage,
		TriggeredAt: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC),
	}
}
