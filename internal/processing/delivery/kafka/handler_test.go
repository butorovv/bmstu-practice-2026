package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/detector"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/storage"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/validator"
)

func TestMessageHandlerHandleValidEventWithoutAlert(t *testing.T) {
	telemetryRepo := storage.NewInMemoryTelemetryRepository()
	alertRepo := storage.NewInMemoryAlertRepository()
	handler := NewMessageHandler(usecase.NewProcessingService(
		telemetryRepo,
		alertRepo,
		detector.New(),
		&testSlidingWindow{},
		&testAlertDeduplicator{reserved: true},
	))

	result, err := handler.Handle(context.Background(), []byte(`{
		"event_id": "day-3-normal-0",
		"device_id": "device-001",
		"patient_id": "patient-001",
		"timestamp": "2026-07-07T12:00:00Z",
		"heart_rate": 78
	}`))
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if result == nil {
		t.Fatal("Handle() result is nil")
	}
	if result.AlertCreated {
		t.Fatal("Handle() created alert for normal heart_rate")
	}

	events, err := telemetryRepo.GetTelemetry(context.Background())
	if err != nil {
		t.Fatalf("GetTelemetry() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("telemetry count = %d, want 1", len(events))
	}

	alerts, err := alertRepo.GetRecentAlerts(context.Background())
	if err != nil {
		t.Fatalf("GetRecentAlerts() error = %v", err)
	}
	if len(alerts) != 0 {
		t.Fatalf("alerts count = %d, want 0", len(alerts))
	}
}

func TestMessageHandlerHandleHighHeartRateCreatesAlert(t *testing.T) {
	telemetryRepo := storage.NewInMemoryTelemetryRepository()
	alertRepo := storage.NewInMemoryAlertRepository()
	latest := model.TelemetryEvent{
		EventID:   "day-3-high-heart-rate-0",
		DeviceID:  "device-001",
		PatientID: "patient-001",
		Timestamp: time.Date(2026, 7, 7, 12, 1, 0, 0, time.UTC),
		HeartRate: 130,
	}
	handler := NewMessageHandler(usecase.NewProcessingService(
		telemetryRepo,
		alertRepo,
		detector.New(),
		&testSlidingWindow{events: []model.TelemetryEvent{
			{
				EventID:   "day-3-high-heart-rate-before",
				DeviceID:  "device-001",
				PatientID: "patient-001",
				Timestamp: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC),
				HeartRate: 130,
			},
			latest,
		}},
		&testAlertDeduplicator{reserved: true},
	))

	result, err := handler.Handle(context.Background(), []byte(`{
		"event_id": "day-3-high-heart-rate-0",
		"device_id": "device-001",
		"patient_id": "patient-001",
		"timestamp": "2026-07-07T12:01:00Z",
		"heart_rate": 130
	}`))
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if result == nil {
		t.Fatal("Handle() result is nil")
	}
	if !result.AlertCreated {
		t.Fatal("Handle() did not create alert for high heart_rate")
	}
	if result.Alert == nil {
		t.Fatal("Handle() alert is nil")
	}
	if result.Alert.Type != model.AlertTypeHighHeartRate {
		t.Fatalf("alert type = %q, want %q", result.Alert.Type, model.AlertTypeHighHeartRate)
	}

	alerts, err := alertRepo.GetAlertsByPatientID(context.Background(), "patient-001")
	if err != nil {
		t.Fatalf("GetAlertsByPatientID() error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("alerts count = %d, want 1", len(alerts))
	}
}

func TestMessageHandlerHandleInvalidJSONDoesNotCallProcessor(t *testing.T) {
	processor := &spyProcessor{}
	handler := NewMessageHandler(processor)

	result, err := handler.Handle(context.Background(), []byte(`{"invalid"`))
	if !errors.Is(err, ErrDecodeMessage) {
		t.Fatalf("Handle() error = %v, want %v", err, ErrDecodeMessage)
	}
	if result != nil {
		t.Fatalf("Handle() result = %+v, want nil", result)
	}
	if processor.calls != 0 {
		t.Fatalf("processor calls = %d, want 0", processor.calls)
	}
}

func TestMessageHandlerHandleInvalidEventReturnsValidationError(t *testing.T) {
	telemetryRepo := storage.NewInMemoryTelemetryRepository()
	alertRepo := storage.NewInMemoryAlertRepository()
	handler := NewMessageHandler(usecase.NewProcessingService(
		telemetryRepo,
		alertRepo,
		detector.New(),
		&testSlidingWindow{},
		&testAlertDeduplicator{reserved: true},
	))

	result, err := handler.Handle(context.Background(), []byte(`{
		"device_id": "",
		"patient_id": ""
	}`))
	if !errors.Is(err, ErrInvalidTelemetryEvent) {
		t.Fatalf("Handle() error = %v, want %v", err, ErrInvalidTelemetryEvent)
	}
	if !errors.Is(err, validator.ErrEmptyEventID) {
		t.Fatalf("Handle() error = %v, want validation error %v", err, validator.ErrEmptyEventID)
	}
	if result != nil {
		t.Fatalf("Handle() result = %+v, want nil", result)
	}
}

func TestMessageHandlerHandleProcessingError(t *testing.T) {
	wantErr := errors.New("processing failed")
	processor := &spyProcessor{err: wantErr}
	handler := NewMessageHandler(processor)

	result, err := handler.Handle(context.Background(), []byte(`{
		"event_id": "day-3-processing-error-0",
		"device_id": "device-001",
		"patient_id": "patient-001",
		"timestamp": "2026-07-07T12:00:00Z",
		"heart_rate": 78
	}`))
	if !errors.Is(err, ErrProcessTelemetryEvent) {
		t.Fatalf("Handle() error = %v, want %v", err, ErrProcessTelemetryEvent)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Handle() error = %v, want wrapped %v", err, wantErr)
	}
	if result != nil {
		t.Fatalf("Handle() result = %+v, want nil", result)
	}
	if processor.calls != 1 {
		t.Fatalf("processor calls = %d, want 1", processor.calls)
	}
}

type spyProcessor struct {
	calls  int
	result *usecase.ProcessingResult
	err    error
}

func (p *spyProcessor) Process(_ context.Context, _ model.TelemetryEvent) (*usecase.ProcessingResult, error) {
	p.calls++
	if p.err != nil {
		return nil, p.err
	}

	return p.result, nil
}

type testSlidingWindow struct {
	events []model.TelemetryEvent
}

func (w *testSlidingWindow) Add(_ context.Context, event model.TelemetryEvent) ([]model.TelemetryEvent, error) {
	if w.events != nil {
		return w.events, nil
	}

	return []model.TelemetryEvent{event}, nil
}

type testAlertDeduplicator struct {
	reserved bool
}

func (d *testAlertDeduplicator) Reserve(_ context.Context, _ string, _ string) (bool, error) {
	return d.reserved, nil
}

func (d *testAlertDeduplicator) Release(_ context.Context, _ string, _ string) error {
	return nil
}
