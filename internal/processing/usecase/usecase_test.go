package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/detector"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/validator"
)

func TestProcessingServiceProcessNormalHeartRate(t *testing.T) {
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New())
	event := validTelemetryEvent(78)

	result, err := service.Process(context.Background(), event)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result == nil {
		t.Fatal("Process() result is nil")
	}
	if result.AlertCreated {
		t.Fatal("Process() created alert for normal heart_rate")
	}
	if result.Alert != nil {
		t.Fatalf("Process() alert = %+v, want nil", result.Alert)
	}
	if len(telemetryRepo.events) != 1 {
		t.Fatalf("saved telemetry count = %d, want 1", len(telemetryRepo.events))
	}
	if len(alertRepo.alerts) != 0 {
		t.Fatalf("saved alerts count = %d, want 0", len(alertRepo.alerts))
	}
}

func TestProcessingServiceProcessHighHeartRateCreatesAlert(t *testing.T) {
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New())
	event := validTelemetryEvent(detector.HighHeartRateThreshold + 1)

	result, err := service.Process(context.Background(), event)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result == nil {
		t.Fatal("Process() result is nil")
	}
	if !result.AlertCreated {
		t.Fatal("Process() did not create alert for high heart_rate")
	}
	if result.Alert == nil {
		t.Fatal("Process() alert is nil")
	}
	if result.Alert.Type != model.AlertTypeHighHeartRate {
		t.Fatalf("alert type = %q, want %q", result.Alert.Type, model.AlertTypeHighHeartRate)
	}
	if result.Alert.Message != model.HighHeartRateMessage {
		t.Fatalf("alert message = %q, want %q", result.Alert.Message, model.HighHeartRateMessage)
	}
	if len(telemetryRepo.events) != 1 {
		t.Fatalf("saved telemetry count = %d, want 1", len(telemetryRepo.events))
	}
	if len(alertRepo.alerts) != 1 {
		t.Fatalf("saved alerts count = %d, want 1", len(alertRepo.alerts))
	}
}

func TestProcessingServiceProcessValidationError(t *testing.T) {
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New())
	event := validTelemetryEvent(78)
	event.EventID = ""

	result, err := service.Process(context.Background(), event)
	if !errors.Is(err, validator.ErrEmptyEventID) {
		t.Fatalf("Process() error = %v, want %v", err, validator.ErrEmptyEventID)
	}
	if result != nil {
		t.Fatalf("Process() result = %+v, want nil", result)
	}
	if len(telemetryRepo.events) != 0 {
		t.Fatalf("saved telemetry count = %d, want 0", len(telemetryRepo.events))
	}
	if len(alertRepo.alerts) != 0 {
		t.Fatalf("saved alerts count = %d, want 0", len(alertRepo.alerts))
	}
}

func TestProcessingServiceProcessTelemetrySaveError(t *testing.T) {
	wantErr := errors.New("telemetry save failed")
	telemetryRepo := &fakeTelemetryRepository{err: wantErr}
	alertRepo := &fakeAlertRepository{}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New())

	result, err := service.Process(context.Background(), validTelemetryEvent(78))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Process() error = %v, want %v", err, wantErr)
	}
	if result != nil {
		t.Fatalf("Process() result = %+v, want nil", result)
	}
	if len(alertRepo.alerts) != 0 {
		t.Fatalf("saved alerts count = %d, want 0", len(alertRepo.alerts))
	}
}

func TestProcessingServiceProcessAlertSaveError(t *testing.T) {
	wantErr := errors.New("alert save failed")
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{saveErr: wantErr}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New())

	result, err := service.Process(context.Background(), validTelemetryEvent(detector.HighHeartRateThreshold+1))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Process() error = %v, want %v", err, wantErr)
	}
	if result != nil {
		t.Fatalf("Process() result = %+v, want nil", result)
	}
	if len(telemetryRepo.events) != 1 {
		t.Fatalf("saved telemetry count = %d, want 1", len(telemetryRepo.events))
	}
	if len(alertRepo.alerts) != 0 {
		t.Fatalf("saved alerts count = %d, want 0", len(alertRepo.alerts))
	}
}

func validTelemetryEvent(heartRate int) model.TelemetryEvent {
	return model.TelemetryEvent{
		EventID:   "event-001",
		DeviceID:  "device-001",
		PatientID: "patient-001",
		Timestamp: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC),
		HeartRate: heartRate,
	}
}

type fakeTelemetryRepository struct {
	events []model.TelemetryEvent
	err    error
}

func (r *fakeTelemetryRepository) SaveTelemetry(_ context.Context, event model.TelemetryEvent) error {
	if r.err != nil {
		return r.err
	}

	r.events = append(r.events, event)

	return nil
}

type fakeAlertRepository struct {
	alerts  []model.Alert
	saveErr error
}

func (r *fakeAlertRepository) SaveAlert(_ context.Context, alert model.Alert) error {
	if r.saveErr != nil {
		return r.saveErr
	}

	r.alerts = append(r.alerts, alert)

	return nil
}

func (r *fakeAlertRepository) GetRecentAlerts(_ context.Context) ([]model.Alert, error) {
	return r.alerts, nil
}

func (r *fakeAlertRepository) GetAlertsByPatientID(_ context.Context, patientID string) ([]model.Alert, error) {
	alerts := make([]model.Alert, 0)
	for _, alert := range r.alerts {
		if alert.PatientID == patientID {
			alerts = append(alerts, alert)
		}
	}

	return alerts, nil
}
