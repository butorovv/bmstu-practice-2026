package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/metrics"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/validator"
)

type TelemetryRepository interface {
	TelemetryWriter
	TelemetryReader
}

type TelemetryWriter interface {
	SaveTelemetry(ctx context.Context, event model.TelemetryEvent) error
}

type TelemetryReader interface {
	ListTelemetry(ctx context.Context, filter TelemetryFilter) ([]model.TelemetryEvent, error)
}

type AlertRepository interface {
	AlertWriter
	AlertReader
}

type AlertWriter interface {
	SaveAlert(ctx context.Context, alert model.Alert) error
	HasRecentAlert(ctx context.Context, patientID string, alertType string, since time.Time) (bool, error)
}

type AlertReader interface {
	ListAlerts(ctx context.Context, filter AlertFilter) ([]model.Alert, error)
}

type TelemetryFilter struct {
	PatientID string
	From      *time.Time
	To        *time.Time
	Limit     int
}

type AlertFilter struct {
	PatientID string
	From      *time.Time
	To        *time.Time
	Limit     int
}

type SlidingWindow interface {
	Add(ctx context.Context, event model.TelemetryEvent) ([]model.TelemetryEvent, error)
}

type AlertDeduplicator interface {
	Reserve(ctx context.Context, patientID string, alertType string) (bool, error)
	Release(ctx context.Context, patientID string, alertType string) error
}

type Detector interface {
	Detect(window []model.TelemetryEvent, latest model.TelemetryEvent) (model.Alert, bool)
}

type ProcessingResult struct {
	Event        model.TelemetryEvent `json:"event"`
	AlertCreated bool                 `json:"alert_created"`
	Alert        *model.Alert         `json:"alert,omitempty"`
}

type ProcessingService struct {
	telemetryRepo TelemetryWriter
	alertRepo     AlertWriter
	detector      Detector
	window        SlidingWindow
	deduplicator  AlertDeduplicator
	dedupWindow   time.Duration
	metrics       metrics.Recorder
}

func NewProcessingService(
	telemetryRepo TelemetryWriter,
	alertRepo AlertWriter,
	detector Detector,
	window SlidingWindow,
	deduplicator AlertDeduplicator,
	dedupWindow ...time.Duration,
) *ProcessingService {
	windowDuration := 5 * time.Minute
	if len(dedupWindow) > 0 && dedupWindow[0] > 0 {
		windowDuration = dedupWindow[0]
	}

	return &ProcessingService{
		telemetryRepo: telemetryRepo,
		alertRepo:     alertRepo,
		detector:      detector,
		window:        window,
		deduplicator:  deduplicator,
		dedupWindow:   windowDuration,
	}
}

func NewProcessingServiceWithMetrics(
	telemetryRepo TelemetryWriter,
	alertRepo AlertWriter,
	detector Detector,
	window SlidingWindow,
	deduplicator AlertDeduplicator,
	recorder metrics.Recorder,
	dedupWindow ...time.Duration,
) *ProcessingService {
	service := NewProcessingService(
		telemetryRepo,
		alertRepo,
		detector,
		window,
		deduplicator,
		dedupWindow...,
	)
	service.metrics = recorder

	return service
}

func (s *ProcessingService) Process(ctx context.Context, event model.TelemetryEvent) (*ProcessingResult, error) {
	startedAt := time.Now()
	defer func() {
		s.observeHistogram("processing_processing_duration_seconds", nil, time.Since(startedAt))
	}()

	if err := validator.ValidateTelemetryEvent(event); err != nil {
		s.recordError("validate")
		return nil, fmt.Errorf("validate telemetry event: %w", err)
	}

	if err := s.telemetryRepo.SaveTelemetry(ctx, event); err != nil {
		s.recordError("postgres")
		return nil, fmt.Errorf("save telemetry: %w", err)
	}

	result := &ProcessingResult{
		Event: event,
	}

	window, err := s.window.Add(ctx, event)
	if err != nil {
		s.recordError("redis")
		return nil, fmt.Errorf("update sliding window: %w", err)
	}
	s.setGauge("processing_sliding_window_events_current", nil, float64(len(window)))

	alert, ok := s.detector.Detect(window, event)
	if !ok {
		return result, nil
	}

	reserved, err := s.deduplicator.Reserve(ctx, event.PatientID, alert.Type)
	if err != nil {
		s.recordError("redis")
		return nil, fmt.Errorf("reserve alert deduplication: %w", err)
	}
	if !reserved {
		hasAlert, err := s.alertRepo.HasRecentAlert(
			ctx,
			alert.PatientID,
			alert.Type,
			time.Now().UTC().Add(-s.dedupWindow),
		)
		if err != nil {
			s.recordError("postgres")
			return nil, fmt.Errorf("check recent alert: %w", err)
		}
		if hasAlert {
			return result, nil
		}

		if err := s.alertRepo.SaveAlert(ctx, alert); err != nil {
			s.recordError("postgres")
			return nil, fmt.Errorf("save alert after dedup recovery: %w", err)
		}

		result.AlertCreated = true
		result.Alert = &alert
		s.recordAlert(alert.Type)

		return result, nil
	}

	if err := s.alertRepo.SaveAlert(ctx, alert); err != nil {
		s.recordError("postgres")
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if releaseErr := s.deduplicator.Release(releaseCtx, event.PatientID, alert.Type); releaseErr != nil {
			s.recordError("redis")
			return nil, fmt.Errorf("save alert: %w; release alert deduplication: %v", err, releaseErr)
		}

		return nil, fmt.Errorf("save alert: %w", err)
	}

	result.AlertCreated = true
	result.Alert = &alert
	s.recordAlert(alert.Type)

	return result, nil
}

func (s *ProcessingService) recordError(stage string) {
	if s.metrics == nil {
		return
	}

	s.metrics.IncCounter("processing_errors_total", metrics.Labels{"stage": stage})
}

func (s *ProcessingService) recordAlert(alertType string) {
	if s.metrics == nil {
		return
	}

	s.metrics.IncCounter("processing_alerts_created_total", metrics.Labels{"type": alertType})
}

func (s *ProcessingService) observeHistogram(name string, labels metrics.Labels, duration time.Duration) {
	if s.metrics == nil {
		return
	}
	if labels == nil {
		labels = metrics.Labels{}
	}

	s.metrics.ObserveHistogram(name, labels, duration.Seconds())
}

func (s *ProcessingService) setGauge(name string, labels metrics.Labels, value float64) {
	if s.metrics == nil {
		return
	}
	if labels == nil {
		labels = metrics.Labels{}
	}

	s.metrics.SetGauge(name, labels, value)
}
