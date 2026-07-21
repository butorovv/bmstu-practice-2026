package generator

import "time"

type Measurement struct {
	Timestamp time.Time `json:"timestamp"`
	HeartRate int       `json:"heart_rate"`
}

type TelemetryBatch struct {
	DeviceID     string        `json:"device_id"`
	PatientID    string        `json:"patient_id"`
	BatchID      string        `json:"batch_id"`
	Measurements []Measurement `json:"measurements"`
}

type acceptedBatchRecord struct {
	BatchID      string    `json:"batch_id"`
	DeviceID     string    `json:"device_id"`
	PatientID    string    `json:"patient_id"`
	EventIDs     []string  `json:"event_ids"`
	Measurements int       `json:"measurements"`
	AcceptedAt   time.Time `json:"accepted_at"`
}
