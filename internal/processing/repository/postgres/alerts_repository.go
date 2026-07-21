package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/metrics"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
)

type AlertRepository struct {
	db      executor
	metrics metrics.Recorder
}

const alertDedupWindow = 5 * time.Minute

var _ usecase.AlertRepository = (*AlertRepository)(nil)

func NewAlertRepository(db executor, recorders ...metrics.Recorder) *AlertRepository {
	var recorder metrics.Recorder
	if len(recorders) > 0 {
		recorder = recorders[0]
	}

	return &AlertRepository{
		db:      db,
		metrics: recorder,
	}
}

func (r *AlertRepository) SaveAlert(ctx context.Context, alert model.Alert) error {
	startedAt := time.Now()
	_, err := r.db.Exec(
		ctx,
		`INSERT INTO alerts (
			patient_id,
			alert_type,
			dedup_key,
			detected_at
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT (dedup_key) DO NOTHING`,
		alert.PatientID,
		alert.Type,
		alertDedupKey(alert),
		alert.TriggeredAt,
	)
	r.observeWrite("save_alert", time.Since(startedAt))

	return err
}

func (r *AlertRepository) HasRecentAlert(
	ctx context.Context,
	patientID string,
	alertType string,
	since time.Time,
) (bool, error) {
	rows, err := r.db.Query(
		ctx,
		`SELECT 1
		FROM alerts
		WHERE patient_id = $1
			AND alert_type = $2
			AND created_at >= $3
		LIMIT 1`,
		patientID,
		alertType,
		since,
	)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), rows.Err()
}

func (r *AlertRepository) ListAlerts(
	ctx context.Context,
	filter usecase.AlertFilter,
) ([]model.Alert, error) {
	query, args := buildAlertsQuery(filter)
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	alerts := make([]model.Alert, 0)
	for rows.Next() {
		var alert model.Alert
		var createdAt time.Time

		if err := rows.Scan(
			&alert.ID,
			&alert.PatientID,
			&alert.Type,
			&alert.TriggeredAt,
			&createdAt,
		); err != nil {
			return nil, err
		}

		alert.TriggeredAt = alert.TriggeredAt.UTC()
		alert.Message = alertMessage(alert.Type)
		createdAt = createdAt.UTC()
		alert.CreatedAt = &createdAt
		alerts = append(alerts, alert)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return alerts, nil
}

func buildAlertsQuery(filter usecase.AlertFilter) (string, []interface{}) {
	query := strings.Builder{}
	query.WriteString(`SELECT id, patient_id, alert_type, detected_at, created_at FROM alerts`)

	where, args := buildCommonWhere(filter.PatientID, filter.From, filter.To, "detected_at")
	if len(where) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(where, " AND "))
	}

	args = append(args, normalizeLimit(filter.Limit))
	query.WriteString(" ORDER BY detected_at DESC LIMIT $")
	query.WriteString(strconv.Itoa(len(args)))

	return query.String(), args
}

func alertMessage(alertType string) string {
	switch alertType {
	case model.AlertTypeHighHeartRate:
		return model.HighHeartRateMessage
	default:
		return ""
	}
}

func alertDedupKey(alert model.Alert) string {
	bucket := alert.TriggeredAt.UTC().Unix() / int64(alertDedupWindow.Seconds())
	return fmt.Sprintf("%s:%s:%d", alert.PatientID, alert.Type, bucket)
}

func (r *AlertRepository) observeWrite(operation string, duration time.Duration) {
	if r.metrics == nil {
		return
	}

	r.metrics.ObserveHistogram(
		"processing_postgres_write_duration_seconds",
		metrics.Labels{"operation": operation},
		duration.Seconds(),
	)
}
