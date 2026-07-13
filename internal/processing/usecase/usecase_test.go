package usecase

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/detector"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/metrics"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/validator"
)

func TestProcessingServiceProcessNormalHeartRate(t *testing.T) {
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{}
	window := &fakeSlidingWindow{}
	deduplicator := &fakeAlertDeduplicator{reserved: true}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New(), window, deduplicator)
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
	if len(telemetryRepo.events) != 1 {
		t.Fatalf("saved telemetry count = %d, want 1", len(telemetryRepo.events))
	}
	if window.calls != 1 {
		t.Fatalf("sliding window calls = %d, want 1", window.calls)
	}
	if deduplicator.calls != 0 {
		t.Fatalf("deduplicator calls = %d, want 0", deduplicator.calls)
	}
	if len(alertRepo.alerts) != 0 {
		t.Fatalf("saved alerts count = %d, want 0", len(alertRepo.alerts))
	}
}

func TestProcessingServiceProcessSingleHighHeartRateDoesNotCreateAlert(t *testing.T) {
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{}
	window := &fakeSlidingWindow{}
	deduplicator := &fakeAlertDeduplicator{reserved: true}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New(), window, deduplicator)

	result, err := service.Process(context.Background(), validTelemetryEvent(detector.HighHeartRateThreshold+1))
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result == nil {
		t.Fatal("Process() result is nil")
	}
	if result.AlertCreated {
		t.Fatal("Process() created alert for a single high measurement")
	}
	if len(alertRepo.alerts) != 0 {
		t.Fatalf("saved alerts count = %d, want 0", len(alertRepo.alerts))
	}
}

func TestProcessingServiceProcessSustainedHighHeartRateCreatesAlert(t *testing.T) {
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{}
	latest := validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime().Add(time.Minute), "event-003")
	window := &fakeSlidingWindow{events: []model.TelemetryEvent{
		validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime(), "event-001"),
		validTelemetryEventAt(detector.HighHeartRateThreshold+2, baseTime().Add(30*time.Second), "event-002"),
		latest,
	}}
	deduplicator := &fakeAlertDeduplicator{reserved: true}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New(), window, deduplicator)

	result, err := service.Process(context.Background(), latest)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result == nil {
		t.Fatal("Process() result is nil")
	}
	if !result.AlertCreated {
		t.Fatal("Process() did not create alert for sustained high heart_rate")
	}
	if result.Alert == nil {
		t.Fatal("Process() alert is nil")
	}
	if result.Alert.Type != model.AlertTypeHighHeartRate {
		t.Fatalf("alert type = %q, want %q", result.Alert.Type, model.AlertTypeHighHeartRate)
	}
	if len(telemetryRepo.events) != 1 {
		t.Fatalf("saved telemetry count = %d, want 1", len(telemetryRepo.events))
	}
	if len(alertRepo.alerts) != 1 {
		t.Fatalf("saved alerts count = %d, want 1", len(alertRepo.alerts))
	}
	if deduplicator.calls != 1 {
		t.Fatalf("deduplicator calls = %d, want 1", deduplicator.calls)
	}
}

func TestProcessingServiceRecordsAlertAndWindowMetrics(t *testing.T) {
	registry := metrics.NewRegistry()
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{}
	latest := validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime().Add(time.Minute), "event-003")
	service := NewProcessingServiceWithMetrics(
		telemetryRepo,
		alertRepo,
		detector.New(),
		&fakeSlidingWindow{events: []model.TelemetryEvent{
			validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime(), "event-001"),
			latest,
		}},
		&fakeAlertDeduplicator{reserved: true},
		registry,
	)

	if _, err := service.Process(context.Background(), latest); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	body := renderMetrics(t, registry)
	assertMetricsTextContains(t, body, `processing_alerts_created_total{type="HIGH_HEART_RATE"} 1`)
	assertMetricsTextContains(t, body, `processing_sliding_window_events_current 2`)
	assertMetricsTextContains(t, body, `processing_processing_duration_seconds_count 1`)
}

func TestProcessingServiceProcessDeduplicatedAlertDoesNotSaveAlert(t *testing.T) {
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{hasRecentAlert: true}
	latest := validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime().Add(time.Minute), "event-003")
	window := &fakeSlidingWindow{events: []model.TelemetryEvent{
		validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime(), "event-001"),
		latest,
	}}
	deduplicator := &fakeAlertDeduplicator{reserved: false}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New(), window, deduplicator)

	result, err := service.Process(context.Background(), latest)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result == nil {
		t.Fatal("Process() result is nil")
	}
	if result.AlertCreated {
		t.Fatal("Process() created duplicate alert")
	}
	if len(alertRepo.alerts) != 0 {
		t.Fatalf("saved alerts count = %d, want 0", len(alertRepo.alerts))
	}
}

func TestProcessingServiceProcessRecoversAlertWhenDedupExistsWithoutPostgresRecord(t *testing.T) {
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{hasRecentAlert: false}
	latest := validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime().Add(time.Minute), "event-003")
	window := &fakeSlidingWindow{events: []model.TelemetryEvent{
		validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime(), "event-001"),
		latest,
	}}
	deduplicator := &fakeAlertDeduplicator{reserved: false}
	service := NewProcessingService(telemetryRepo, alertRepo, detector.New(), window, deduplicator)

	result, err := service.Process(context.Background(), latest)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result == nil {
		t.Fatal("Process() result is nil")
	}
	if !result.AlertCreated {
		t.Fatal("Process() did not recover missing alert")
	}
	if len(alertRepo.alerts) != 1 {
		t.Fatalf("saved alerts count = %d, want 1", len(alertRepo.alerts))
	}
}

func TestProcessingServiceProcessValidationError(t *testing.T) {
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{}
	service := NewProcessingService(
		telemetryRepo,
		alertRepo,
		detector.New(),
		&fakeSlidingWindow{},
		&fakeAlertDeduplicator{reserved: true},
	)
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
}

func TestProcessingServiceProcessTelemetrySaveError(t *testing.T) {
	wantErr := errors.New("telemetry save failed")
	telemetryRepo := &fakeTelemetryRepository{err: wantErr}
	alertRepo := &fakeAlertRepository{}
	window := &fakeSlidingWindow{}
	service := NewProcessingService(
		telemetryRepo,
		alertRepo,
		detector.New(),
		window,
		&fakeAlertDeduplicator{reserved: true},
	)

	result, err := service.Process(context.Background(), validTelemetryEvent(78))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Process() error = %v, want %v", err, wantErr)
	}
	if result != nil {
		t.Fatalf("Process() result = %+v, want nil", result)
	}
	if window.calls != 0 {
		t.Fatalf("sliding window calls = %d, want 0", window.calls)
	}
}

func TestProcessingServiceProcessSlidingWindowError(t *testing.T) {
	wantErr := errors.New("redis unavailable")
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{}
	window := &fakeSlidingWindow{err: wantErr}
	service := NewProcessingService(
		telemetryRepo,
		alertRepo,
		detector.New(),
		window,
		&fakeAlertDeduplicator{reserved: true},
	)

	result, err := service.Process(context.Background(), validTelemetryEvent(78))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Process() error = %v, want %v", err, wantErr)
	}
	if result != nil {
		t.Fatalf("Process() result = %+v, want nil", result)
	}
}

func TestProcessingServiceProcessAlertSaveErrorReleasesDeduplication(t *testing.T) {
	wantErr := errors.New("alert save failed")
	telemetryRepo := &fakeTelemetryRepository{}
	alertRepo := &fakeAlertRepository{saveErr: wantErr}
	latest := validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime().Add(time.Minute), "event-003")
	deduplicator := &fakeAlertDeduplicator{reserved: true}
	service := NewProcessingService(
		telemetryRepo,
		alertRepo,
		detector.New(),
		&fakeSlidingWindow{events: []model.TelemetryEvent{
			validTelemetryEventAt(detector.HighHeartRateThreshold+1, baseTime(), "event-001"),
			latest,
		}},
		deduplicator,
	)

	result, err := service.Process(context.Background(), latest)
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
	if deduplicator.releaseCalls != 1 {
		t.Fatalf("deduplication release calls = %d, want 1", deduplicator.releaseCalls)
	}
}

func validTelemetryEvent(heartRate int) model.TelemetryEvent {
	return validTelemetryEventAt(heartRate, baseTime(), "event-001")
}

func validTelemetryEventAt(heartRate int, timestamp time.Time, eventID string) model.TelemetryEvent {
	return model.TelemetryEvent{
		EventID:   eventID,
		DeviceID:  "device-001",
		PatientID: "patient-001",
		Timestamp: timestamp,
		HeartRate: heartRate,
	}
}

func baseTime() time.Time {
	return time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
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
	alerts         []model.Alert
	saveErr        error
	hasRecentAlert bool
	hasRecentErr   error
}

func (r *fakeAlertRepository) SaveAlert(_ context.Context, alert model.Alert) error {
	if r.saveErr != nil {
		return r.saveErr
	}

	r.alerts = append(r.alerts, alert)

	return nil
}

func (r *fakeAlertRepository) HasRecentAlert(
	_ context.Context,
	_ string,
	_ string,
	_ time.Time,
) (bool, error) {
	if r.hasRecentErr != nil {
		return false, r.hasRecentErr
	}

	return r.hasRecentAlert, nil
}

type fakeSlidingWindow struct {
	events []model.TelemetryEvent
	err    error
	calls  int
}

func (w *fakeSlidingWindow) Add(_ context.Context, event model.TelemetryEvent) ([]model.TelemetryEvent, error) {
	w.calls++
	if w.err != nil {
		return nil, w.err
	}
	if w.events != nil {
		return w.events, nil
	}

	return []model.TelemetryEvent{event}, nil
}

type fakeAlertDeduplicator struct {
	reserved     bool
	err          error
	releaseErr   error
	calls        int
	releaseCalls int
}

func (d *fakeAlertDeduplicator) Reserve(_ context.Context, _ string, _ string) (bool, error) {
	d.calls++
	if d.err != nil {
		return false, d.err
	}

	return d.reserved, nil
}

func (d *fakeAlertDeduplicator) Release(_ context.Context, _ string, _ string) error {
	d.releaseCalls++
	return d.releaseErr
}

func renderMetrics(t *testing.T, registry *metrics.Registry) string {
	t.Helper()

	var buffer bytes.Buffer
	if _, err := registry.WriteTo(&buffer); err != nil {
		t.Fatalf("WriteTo() error = %v", err)
	}

	return buffer.String()
}

func assertMetricsTextContains(t *testing.T, body string, want string) {
	t.Helper()

	if !strings.Contains(body, want) {
		t.Fatalf("metrics body does not contain %q:\n%s", want, body)
	}
}
