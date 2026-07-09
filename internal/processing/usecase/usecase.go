package usecase

import (
	"context"
	"fmt"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/validator"
)

type TelemetryRepository interface {
	SaveTelemetry(ctx context.Context, event model.TelemetryEvent) error
}

type AlertRepository interface {
	SaveAlert(ctx context.Context, alert model.Alert) error
	GetRecentAlerts(ctx context.Context) ([]model.Alert, error)
	GetAlertsByPatientID(ctx context.Context, patientID string) ([]model.Alert, error)
}

type Detector interface {
	Detect(event model.TelemetryEvent) (model.Alert, bool)
}

type ProcessingResult struct {
	Event        model.TelemetryEvent `json:"event"`
	AlertCreated bool                 `json:"alert_created"`
	Alert        *model.Alert         `json:"alert,omitempty"`
}

type ProcessingService struct {
	telemetryRepo TelemetryRepository
	alertRepo     AlertRepository
	detector      Detector
}

func NewProcessingService(
	telemetryRepo TelemetryRepository,
	alertRepo AlertRepository,
	detector Detector,
) *ProcessingService {
	return &ProcessingService{
		telemetryRepo: telemetryRepo,
		alertRepo:     alertRepo,
		detector:      detector,
	}
}

func (s *ProcessingService) Process(ctx context.Context, event model.TelemetryEvent) (*ProcessingResult, error) {
	if err := validator.ValidateTelemetryEvent(event); err != nil {
		return nil, fmt.Errorf("validate telemetry event: %w", err)
	}

	if err := s.telemetryRepo.SaveTelemetry(ctx, event); err != nil {
		return nil, fmt.Errorf("save telemetry: %w", err)
	}

	result := &ProcessingResult{
		Event: event,
	}

	alert, ok := s.detector.Detect(event)
	if !ok {
		return result, nil
	}

	if err := s.alertRepo.SaveAlert(ctx, alert); err != nil {
		return nil, fmt.Errorf("save alert: %w", err)
	}

	result.AlertCreated = true
	result.Alert = &alert

	return result, nil
}
