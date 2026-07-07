package validator

import (
	"errors"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
)

const (
	minHeartRate    = 20
	maxHeartRate    = 250
	maxMeasurements = 10
	minMeasurements = 1
)

var (
	ErrEmptyDeviceID        = errors.New("device_id is required")
	ErrEmptyPatientID       = errors.New("patient_id is required")
	ErrEmptyBatchID         = errors.New("batch_id is required")
	ErrEmptyMeasurements    = errors.New("measurements length must be between 1 and 10")
	ErrTooManyMeasurements  = errors.New("measurements length must be between 1 and 10")
	ErrEmptyTimestamp       = errors.New("measurement timestamp is required")
	ErrHeartRateTooLow      = errors.New("heart_rate must be greater than or equal to 20")
	ErrHeartRateTooHigh     = errors.New("heart_rate must be less than or equal to 250")
	ErrTimestampsOutOfOrder = errors.New("measurement timestamps must be strictly increasing")
)

type Validator struct{}

func New() Validator {
	return Validator{}
}

func (Validator) ValidateBatch(batch usecase.TelemetryBatch) error {
	if batch.DeviceID == "" {
		return ErrEmptyDeviceID
	}
	if batch.PatientID == "" {
		return ErrEmptyPatientID
	}
	if batch.BatchID == "" {
		return ErrEmptyBatchID
	}
	if len(batch.Measurements) < minMeasurements {
		return ErrEmptyMeasurements
	}
	if len(batch.Measurements) > maxMeasurements {
		return ErrTooManyMeasurements
	}

	for i, measurement := range batch.Measurements {
		if measurement.Timestamp.IsZero() {
			return ErrEmptyTimestamp
		}
		if measurement.HeartRate < minHeartRate {
			return ErrHeartRateTooLow
		}
		if measurement.HeartRate > maxHeartRate {
			return ErrHeartRateTooHigh
		}
		if i > 0 && !measurement.Timestamp.After(batch.Measurements[i-1].Timestamp) {
			return ErrTimestampsOutOfOrder
		}
	}

	return nil
}
