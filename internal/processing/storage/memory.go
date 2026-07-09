package storage

import (
	"context"
	"sync"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
)

type InMemoryTelemetryRepository struct {
	mu        sync.RWMutex
	telemetry []model.TelemetryEvent
}

func NewInMemoryTelemetryRepository() *InMemoryTelemetryRepository {
	return &InMemoryTelemetryRepository{
		telemetry: make([]model.TelemetryEvent, 0),
	}
}

func (r *InMemoryTelemetryRepository) SaveTelemetry(ctx context.Context, event model.TelemetryEvent) error {
	if err := contextError(ctx); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.telemetry = append(r.telemetry, event)

	return nil
}

func (r *InMemoryTelemetryRepository) GetTelemetry(ctx context.Context) ([]model.TelemetryEvent, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	events := make([]model.TelemetryEvent, len(r.telemetry))
	copy(events, r.telemetry)

	return events, nil
}

type InMemoryAlertRepository struct {
	mu     sync.RWMutex
	alerts []model.Alert
	nextID int64
}

func NewInMemoryAlertRepository() *InMemoryAlertRepository {
	return &InMemoryAlertRepository{
		alerts: make([]model.Alert, 0),
		nextID: 1,
	}
}

func (r *InMemoryAlertRepository) SaveAlert(ctx context.Context, alert model.Alert) error {
	if err := contextError(ctx); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if alert.ID == 0 {
		alert.ID = r.nextID
		r.nextID++
	} else if alert.ID >= r.nextID {
		r.nextID = alert.ID + 1
	}

	r.alerts = append(r.alerts, alert)

	return nil
}

func (r *InMemoryAlertRepository) GetRecentAlerts(ctx context.Context) ([]model.Alert, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	alerts := make([]model.Alert, len(r.alerts))
	copy(alerts, r.alerts)

	return alerts, nil
}

func (r *InMemoryAlertRepository) GetAlertsByPatientID(ctx context.Context, patientID string) ([]model.Alert, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	alerts := make([]model.Alert, 0)
	for _, alert := range r.alerts {
		if alert.PatientID == patientID {
			alerts = append(alerts, alert)
		}
	}

	return alerts, nil
}

func contextError(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
