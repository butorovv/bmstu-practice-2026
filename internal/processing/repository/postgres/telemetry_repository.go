package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/metrics"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
)

const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

type TelemetryRepository struct {
	db      executor
	metrics metrics.Recorder
}

var _ usecase.TelemetryRepository = (*TelemetryRepository)(nil)

func NewTelemetryRepository(db executor, recorders ...metrics.Recorder) *TelemetryRepository {
	var recorder metrics.Recorder
	if len(recorders) > 0 {
		recorder = recorders[0]
	}

	return &TelemetryRepository{
		db:      db,
		metrics: recorder,
	}
}

func (r *TelemetryRepository) SaveTelemetry(ctx context.Context, event model.TelemetryEvent) error {
	startedAt := time.Now()
	_, err := r.db.Exec(
		ctx,
		`INSERT INTO telemetry (
			event_id,
			patient_id,
			device_id,
			"timestamp",
			heart_rate,
			spo2,
			temperature
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (event_id) DO NOTHING`,
		event.EventID,
		event.PatientID,
		event.DeviceID,
		event.Timestamp,
		event.HeartRate,
		nullableInt(event.SpO2),
		nullableFloat(event.Temperature),
	)
	r.observeWrite("save_telemetry", time.Since(startedAt))

	return err
}

func (r *TelemetryRepository) ListTelemetry(
	ctx context.Context,
	filter usecase.TelemetryFilter,
) ([]model.TelemetryEvent, error) {
	query, args := buildTelemetryQuery(filter)
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]model.TelemetryEvent, 0)
	for rows.Next() {
		var event model.TelemetryEvent
		var spo2 sql.NullInt64
		var temperature sql.NullFloat64
		var createdAt time.Time

		if err := rows.Scan(
			&event.EventID,
			&event.DeviceID,
			&event.PatientID,
			&event.Timestamp,
			&event.HeartRate,
			&spo2,
			&temperature,
			&createdAt,
		); err != nil {
			return nil, err
		}

		event.Timestamp = event.Timestamp.UTC()
		if spo2.Valid {
			value := int(spo2.Int64)
			event.SpO2 = &value
		}
		if temperature.Valid {
			value := temperature.Float64
			event.Temperature = &value
		}
		createdAt = createdAt.UTC()
		event.CreatedAt = &createdAt

		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func buildTelemetryQuery(filter usecase.TelemetryFilter) (string, []interface{}) {
	query := strings.Builder{}
	query.WriteString(`SELECT event_id, device_id, patient_id, "timestamp", heart_rate, spo2, temperature, created_at FROM telemetry`)

	where, args := buildCommonWhere(filter.PatientID, filter.From, filter.To, `"timestamp"`)
	if len(where) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(where, " AND "))
	}

	args = append(args, normalizeLimit(filter.Limit))
	query.WriteString(` ORDER BY "timestamp" DESC LIMIT $`)
	query.WriteString(strconv.Itoa(len(args)))

	return query.String(), args
}

func buildCommonWhere(
	patientID string,
	from *time.Time,
	to *time.Time,
	timeColumn string,
) ([]string, []interface{}) {
	where := make([]string, 0, 3)
	args := make([]interface{}, 0, 4)

	if patientID != "" {
		args = append(args, patientID)
		where = append(where, fmt.Sprintf("patient_id = $%d", len(args)))
	}
	if from != nil {
		args = append(args, *from)
		where = append(where, fmt.Sprintf("%s >= $%d", timeColumn, len(args)))
	}
	if to != nil {
		args = append(args, *to)
		where = append(where, fmt.Sprintf("%s <= $%d", timeColumn, len(args)))
	}

	return where, args
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return defaultListLimit
	}
	if limit > maxListLimit {
		return maxListLimit
	}

	return limit
}

func nullableInt(value *int) interface{} {
	if value == nil {
		return nil
	}

	return *value
}

func nullableFloat(value *float64) interface{} {
	if value == nil {
		return nil
	}

	return *value
}

func (r *TelemetryRepository) observeWrite(operation string, duration time.Duration) {
	if r.metrics == nil {
		return
	}

	r.metrics.ObserveHistogram(
		"processing_postgres_write_duration_seconds",
		metrics.Labels{"operation": operation},
		duration.Seconds(),
	)
}
