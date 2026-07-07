package usecase

import (
	"fmt"
	"time"
)

type TelemetryBatch struct {
	DeviceID     string        `json:"device_id"`
	PatientID    string        `json:"patient_id"`
	BatchID      string        `json:"batch_id"`
	Measurements []Measurement `json:"measurements"`
}

type Measurement struct {
	Timestamp time.Time `json:"timestamp"`
	HeartRate int       `json:"heart_rate"`
}

type TelemetryEvent struct {
	EventID   string    `json:"event_id"`
	DeviceID  string    `json:"device_id"`
	PatientID string    `json:"patient_id"`
	Timestamp time.Time `json:"timestamp"`
	HeartRate int       `json:"heart_rate"`
}

func BuildEvents(batch TelemetryBatch) []TelemetryEvent {
	events := make([]TelemetryEvent, 0, len(batch.Measurements))

	for i, measurement := range batch.Measurements {
		events = append(events, TelemetryEvent{
			EventID:   fmt.Sprintf("%s-%d", batch.BatchID, i),
			DeviceID:  batch.DeviceID,
			PatientID: batch.PatientID,
			Timestamp: measurement.Timestamp,
			HeartRate: measurement.HeartRate,
		})
	}

	return events
}
