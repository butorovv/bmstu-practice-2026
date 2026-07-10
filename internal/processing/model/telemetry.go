package model

import "time"

const (
	AlertTypeHighHeartRate = "HIGH_HEART_RATE"
	HighHeartRateMessage   = "Patient has high heart rate"

	AlertMessageHighHeartRate = HighHeartRateMessage
)

type TelemetryEvent struct {
	EventID     string     `json:"event_id"`
	DeviceID    string     `json:"device_id"`
	PatientID   string     `json:"patient_id"`
	Timestamp   time.Time  `json:"timestamp"`
	HeartRate   int        `json:"heart_rate"`
	SpO2        *int       `json:"spo2,omitempty"`
	Temperature *float64   `json:"temperature,omitempty"`
	CreatedAt   *time.Time `json:"created_at,omitempty"`
}

type Alert struct {
	ID          int64      `json:"id"`
	PatientID   string     `json:"patient_id"`
	Type        string     `json:"type"`
	Message     string     `json:"message"`
	TriggeredAt time.Time  `json:"triggered_at"`
	CreatedAt   *time.Time `json:"created_at,omitempty"`
}
