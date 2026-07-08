package validator

import (
	"errors"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
)

func TestValidateTelemetryEvent(t *testing.T) {
	baseTime := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)

	validEvent := func() model.TelemetryEvent {
		return model.TelemetryEvent{
			EventID:   "event-001",
			DeviceID:  "device-001",
			PatientID: "patient-001",
			Timestamp: baseTime,
			HeartRate: 78,
		}
	}

	tests := []struct {
		name    string
		mutate  func(*model.TelemetryEvent)
		wantErr error
	}{
		{
			name: "valid event",
		},
		{
			name: "empty event_id",
			mutate: func(event *model.TelemetryEvent) {
				event.EventID = ""
			},
			wantErr: ErrEmptyEventID,
		},
		{
			name: "empty device_id",
			mutate: func(event *model.TelemetryEvent) {
				event.DeviceID = ""
			},
			wantErr: ErrEmptyDeviceID,
		},
		{
			name: "empty patient_id",
			mutate: func(event *model.TelemetryEvent) {
				event.PatientID = ""
			},
			wantErr: ErrEmptyPatientID,
		},
		{
			name: "empty timestamp",
			mutate: func(event *model.TelemetryEvent) {
				event.Timestamp = time.Time{}
			},
			wantErr: ErrEmptyTimestamp,
		},
		{
			name: "timestamp is not UTC",
			mutate: func(event *model.TelemetryEvent) {
				event.Timestamp = time.Date(2026, 7, 7, 15, 0, 0, 0, time.FixedZone("MSK", 3*60*60))
			},
			wantErr: ErrTimestampNotUTC,
		},
		{
			name: "heart_rate less than 20",
			mutate: func(event *model.TelemetryEvent) {
				event.HeartRate = 19
			},
			wantErr: ErrHeartRateTooLow,
		},
		{
			name: "heart_rate equals 20",
			mutate: func(event *model.TelemetryEvent) {
				event.HeartRate = 20
			},
		},
		{
			name: "heart_rate equals 250",
			mutate: func(event *model.TelemetryEvent) {
				event.HeartRate = 250
			},
		},
		{
			name: "heart_rate greater than 250",
			mutate: func(event *model.TelemetryEvent) {
				event.HeartRate = 251
			},
			wantErr: ErrHeartRateTooHigh,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := validEvent()
			if tt.mutate != nil {
				tt.mutate(&event)
			}

			err := ValidateTelemetryEvent(event)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateTelemetryEvent() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
