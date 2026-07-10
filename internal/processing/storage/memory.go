package storage

import (
	"context"
	"sync"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
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
	return r.ListTelemetry(ctx, usecase.TelemetryFilter{})
}

func (r *InMemoryTelemetryRepository) ListTelemetry(
	ctx context.Context,
	filter usecase.TelemetryFilter,
) ([]model.TelemetryEvent, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	events := make([]model.TelemetryEvent, 0, len(r.telemetry))
	for _, event := range r.telemetry {
		if !matchesTelemetryFilter(event, filter) {
			continue
		}
		events = append(events, event)
		if filter.Limit > 0 && len(events) >= filter.Limit {
			break
		}
	}

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

func (r *InMemoryAlertRepository) HasRecentAlert(
	ctx context.Context,
	patientID string,
	alertType string,
	since time.Time,
) (bool, error) {
	if err := contextError(ctx); err != nil {
		return false, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, alert := range r.alerts {
		createdAt := alert.TriggeredAt
		if alert.CreatedAt != nil {
			createdAt = *alert.CreatedAt
		}
		if alert.PatientID == patientID &&
			alert.Type == alertType &&
			!createdAt.Before(since) {
			return true, nil
		}
	}

	return false, nil
}

func (r *InMemoryAlertRepository) GetRecentAlerts(ctx context.Context) ([]model.Alert, error) {
	return r.ListAlerts(ctx, usecase.AlertFilter{})
}

func (r *InMemoryAlertRepository) ListAlerts(ctx context.Context, filter usecase.AlertFilter) ([]model.Alert, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	alerts := make([]model.Alert, 0, len(r.alerts))
	for _, alert := range r.alerts {
		if !matchesAlertFilter(alert, filter) {
			continue
		}
		alerts = append(alerts, alert)
		if filter.Limit > 0 && len(alerts) >= filter.Limit {
			break
		}
	}

	return alerts, nil
}

func (r *InMemoryAlertRepository) GetAlertsByPatientID(ctx context.Context, patientID string) ([]model.Alert, error) {
	return r.ListAlerts(ctx, usecase.AlertFilter{PatientID: patientID})
}

func matchesTelemetryFilter(event model.TelemetryEvent, filter usecase.TelemetryFilter) bool {
	return matchesCommonFilter(event.PatientID, event.Timestamp, filter.PatientID, filter.From, filter.To)
}

func matchesAlertFilter(alert model.Alert, filter usecase.AlertFilter) bool {
	return matchesCommonFilter(alert.PatientID, alert.TriggeredAt, filter.PatientID, filter.From, filter.To)
}

func matchesCommonFilter(
	patientID string,
	timestamp time.Time,
	filterPatientID string,
	from *time.Time,
	to *time.Time,
) bool {
	if filterPatientID != "" && patientID != filterPatientID {
		return false
	}
	if from != nil && timestamp.Before(*from) {
		return false
	}
	if to != nil && timestamp.After(*to) {
		return false
	}

	return true
}

func contextError(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
