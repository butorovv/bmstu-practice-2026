package postgres

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoriesIntegration(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN is not set")
	}

	ctx := context.Background()
	pool := newIsolatedPostgresPool(t, ctx, dsn)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	telemetryRepo := NewTelemetryRepository(pool)
	alertRepo := NewAlertRepository(pool)
	cleanupRepo := NewCleanupRepository(pool)
	baseTime := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	spo2 := 98
	temperature := 36.6
	event := model.TelemetryEvent{
		EventID:     "event-001",
		DeviceID:    "device-001",
		PatientID:   "patient-001",
		Timestamp:   baseTime,
		HeartRate:   130,
		SpO2:        &spo2,
		Temperature: &temperature,
	}

	if err := telemetryRepo.SaveTelemetry(ctx, event); err != nil {
		t.Fatalf("SaveTelemetry() error = %v", err)
	}
	if err := telemetryRepo.SaveTelemetry(ctx, event); err != nil {
		t.Fatalf("SaveTelemetry() duplicate error = %v", err)
	}

	events, err := telemetryRepo.ListTelemetry(ctx, usecase.TelemetryFilter{
		PatientID: event.PatientID,
		From:      ptrTime(baseTime.Add(-time.Second)),
		To:        ptrTime(baseTime.Add(time.Second)),
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListTelemetry() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("telemetry count = %d, want 1", len(events))
	}
	if events[0].EventID != event.EventID {
		t.Fatalf("event_id = %q, want %q", events[0].EventID, event.EventID)
	}
	if events[0].SpO2 == nil || *events[0].SpO2 != spo2 {
		t.Fatalf("spo2 = %v, want %d", events[0].SpO2, spo2)
	}

	alert := model.Alert{
		PatientID:   event.PatientID,
		Type:        model.AlertTypeHighHeartRate,
		Message:     model.HighHeartRateMessage,
		TriggeredAt: baseTime,
	}
	if err := alertRepo.SaveAlert(ctx, alert); err != nil {
		t.Fatalf("SaveAlert() error = %v", err)
	}

	alerts, err := alertRepo.ListAlerts(ctx, usecase.AlertFilter{
		PatientID: event.PatientID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListAlerts() error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("alerts count = %d, want 1", len(alerts))
	}
	if alerts[0].Type != model.AlertTypeHighHeartRate {
		t.Fatalf("alert type = %q, want %q", alerts[0].Type, model.AlertTypeHighHeartRate)
	}
	if alerts[0].Message != model.HighHeartRateMessage {
		t.Fatalf("alert message = %q, want %q", alerts[0].Message, model.HighHeartRateMessage)
	}

	if _, err := pool.Exec(
		ctx,
		`INSERT INTO telemetry (event_id, patient_id, device_id, "timestamp", heart_rate, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW() - INTERVAL '31 days')`,
		"old-event",
		"old-patient",
		"device-001",
		baseTime,
		80,
	); err != nil {
		t.Fatalf("insert old telemetry: %v", err)
	}
	if _, err := pool.Exec(
		ctx,
		`INSERT INTO alerts (patient_id, alert_type, dedup_key, detected_at, created_at)
		VALUES ($1, $2, $3, $4, NOW() - INTERVAL '31 days')`,
		"old-patient",
		model.AlertTypeHighHeartRate,
		"old-patient:HIGH_HEART_RATE:old",
		baseTime,
	); err != nil {
		t.Fatalf("insert old alert: %v", err)
	}

	if err := cleanupRepo.DeleteOlderThan(ctx, time.Now().UTC().Add(-30*24*time.Hour)); err != nil {
		t.Fatalf("DeleteOlderThan() error = %v", err)
	}

	oldEvents, err := telemetryRepo.ListTelemetry(ctx, usecase.TelemetryFilter{PatientID: "old-patient", Limit: 10})
	if err != nil {
		t.Fatalf("ListTelemetry(old) error = %v", err)
	}
	if len(oldEvents) != 0 {
		t.Fatalf("old telemetry count = %d, want 0", len(oldEvents))
	}
	oldAlerts, err := alertRepo.ListAlerts(ctx, usecase.AlertFilter{PatientID: "old-patient", Limit: 10})
	if err != nil {
		t.Fatalf("ListAlerts(old) error = %v", err)
	}
	if len(oldAlerts) != 0 {
		t.Fatalf("old alerts count = %d, want 0", len(oldAlerts))
	}
}

func TestAlertRepositoryConcurrentDedupKey(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN is not set")
	}

	ctx := context.Background()
	pool := newIsolatedPostgresPool(t, ctx, dsn)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	alertRepo := NewAlertRepository(pool)
	alert := model.Alert{
		PatientID:   "patient-race",
		Type:        model.AlertTypeHighHeartRate,
		Message:     model.HighHeartRateMessage,
		TriggeredAt: time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC),
	}

	const workers = 2
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- alertRepo.SaveAlert(ctx, alert)
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("SaveAlert() concurrent error = %v", err)
		}
	}

	alerts, err := alertRepo.ListAlerts(ctx, usecase.AlertFilter{
		PatientID: alert.PatientID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListAlerts() error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("alerts count = %d, want 1", len(alerts))
	}
}

func newIsolatedPostgresPool(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()

	adminPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect admin postgres: %v", err)
	}
	t.Cleanup(adminPool.Close)

	schema := fmt.Sprintf("processing_test_%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	})

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse postgres dsn: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect isolated postgres: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
