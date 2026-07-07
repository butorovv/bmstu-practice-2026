package validator

import (
	"errors"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
)

func TestValidatorValidateBatch(t *testing.T) {
	baseTime := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)

	validBatch := func() usecase.TelemetryBatch {
		return usecase.TelemetryBatch{
			DeviceID:  "device-001",
			PatientID: "patient-001",
			BatchID:   "device-001-000001",
			Measurements: []usecase.Measurement{
				{Timestamp: baseTime, HeartRate: 78},
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*usecase.TelemetryBatch)
		wantErr error
	}{
		{
			name: "valid batch",
		},
		{
			name: "empty device_id",
			mutate: func(batch *usecase.TelemetryBatch) {
				batch.DeviceID = ""
			},
			wantErr: ErrEmptyDeviceID,
		},
		{
			name: "empty patient_id",
			mutate: func(batch *usecase.TelemetryBatch) {
				batch.PatientID = ""
			},
			wantErr: ErrEmptyPatientID,
		},
		{
			name: "empty batch_id",
			mutate: func(batch *usecase.TelemetryBatch) {
				batch.BatchID = ""
			},
			wantErr: ErrEmptyBatchID,
		},
		{
			name: "empty measurements",
			mutate: func(batch *usecase.TelemetryBatch) {
				batch.Measurements = nil
			},
			wantErr: ErrEmptyMeasurements,
		},
		{
			name: "11 measurements",
			mutate: func(batch *usecase.TelemetryBatch) {
				batch.Measurements = make([]usecase.Measurement, 11)
				for i := range batch.Measurements {
					batch.Measurements[i] = usecase.Measurement{
						Timestamp: baseTime.Add(time.Duration(i) * time.Second),
						HeartRate: 78,
					}
				}
			},
			wantErr: ErrTooManyMeasurements,
		},
		{
			name: "heart_rate less than 20",
			mutate: func(batch *usecase.TelemetryBatch) {
				batch.Measurements[0].HeartRate = 19
			},
			wantErr: ErrHeartRateTooLow,
		},
		{
			name: "empty timestamp",
			mutate: func(batch *usecase.TelemetryBatch) {
				batch.Measurements[0].Timestamp = time.Time{}
			},
			wantErr: ErrEmptyTimestamp,
		},
		{
			name: "heart_rate greater than 250",
			mutate: func(batch *usecase.TelemetryBatch) {
				batch.Measurements[0].HeartRate = 251
			},
			wantErr: ErrHeartRateTooHigh,
		},
		{
			name: "timestamps out of order",
			mutate: func(batch *usecase.TelemetryBatch) {
				batch.Measurements = []usecase.Measurement{
					{Timestamp: baseTime.Add(time.Second), HeartRate: 78},
					{Timestamp: baseTime, HeartRate: 80},
				}
			},
			wantErr: ErrTimestampsOutOfOrder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := validBatch()
			if tt.mutate != nil {
				tt.mutate(&batch)
			}

			err := New().ValidateBatch(batch)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateBatch() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
