package detector

import (
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
)

func TestDetectorDetect(t *testing.T) {
	baseTime := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		heartRate int
		wantAlert bool
	}{
		{
			name:      "normal heart rate",
			heartRate: 78,
		},
		{
			name:      "threshold does not trigger",
			heartRate: HighHeartRateThreshold,
		},
		{
			name:      "high heart rate triggers alert",
			heartRate: HighHeartRateThreshold + 1,
			wantAlert: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := model.TelemetryEvent{
				EventID:   "event-001",
				DeviceID:  "device-001",
				PatientID: "patient-001",
				Timestamp: baseTime,
				HeartRate: tt.heartRate,
			}

			alert, ok := New().Detect(event)
			if ok != tt.wantAlert {
				t.Fatalf("Detect() alert = %v, want %v", ok, tt.wantAlert)
			}
			if !ok {
				return
			}
			if alert.PatientID != event.PatientID {
				t.Fatalf("alert patient_id = %q, want %q", alert.PatientID, event.PatientID)
			}
			if alert.Type != model.AlertTypeHighHeartRate {
				t.Fatalf("alert type = %q, want %q", alert.Type, model.AlertTypeHighHeartRate)
			}
			if alert.Message != model.HighHeartRateMessage {
				t.Fatalf("alert message = %q, want %q", alert.Message, model.HighHeartRateMessage)
			}
			if !alert.TriggeredAt.Equal(event.Timestamp) {
				t.Fatalf("alert triggered_at = %v, want %v", alert.TriggeredAt, event.Timestamp)
			}
		})
	}
}
