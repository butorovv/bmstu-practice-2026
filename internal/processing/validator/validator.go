package validator

import (
	"errors"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
)

const (
	minHeartRate = 20
	maxHeartRate = 250
)

var (
	ErrEmptyEventID     = errors.New("event_id is required")
	ErrEmptyDeviceID    = errors.New("device_id is required")
	ErrEmptyPatientID   = errors.New("patient_id is required")
	ErrEmptyTimestamp   = errors.New("timestamp is required")
	ErrTimestampNotUTC  = errors.New("timestamp must be RFC3339 UTC")
	ErrHeartRateTooLow  = errors.New("heart_rate must be greater than or equal to 20")
	ErrHeartRateTooHigh = errors.New("heart_rate must be less than or equal to 250")
)

type Validator struct{}

func New() Validator {
	return Validator{}
}

func (Validator) ValidateTelemetryEvent(event model.TelemetryEvent) error {
	return ValidateTelemetryEvent(event)
}

func ValidateTelemetryEvent(event model.TelemetryEvent) error {
	if event.EventID == "" {
		return ErrEmptyEventID
	}
	if event.DeviceID == "" {
		return ErrEmptyDeviceID
	}
	if event.PatientID == "" {
		return ErrEmptyPatientID
	}
	if event.Timestamp.IsZero() {
		return ErrEmptyTimestamp
	}
	if _, offset := event.Timestamp.Zone(); offset != 0 {
		return ErrTimestampNotUTC
	}
	if event.HeartRate < minHeartRate {
		return ErrHeartRateTooLow
	}
	if event.HeartRate > maxHeartRate {
		return ErrHeartRateTooHigh
	}

	return nil
}
