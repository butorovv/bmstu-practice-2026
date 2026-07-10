package detector

import (
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
)

func TestDetectorDetect(t *testing.T) {
	baseTime := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	latestHigh := testEvent("event-003", baseTime.Add(time.Minute), HighHeartRateThreshold+1)

	tests := []struct {
		name      string
		window    []model.TelemetryEvent
		latest    model.TelemetryEvent
		wantAlert bool
	}{
		{
			name:   "normal latest heart rate",
			window: []model.TelemetryEvent{testEvent("event-001", baseTime, 78)},
			latest: testEvent("event-001", baseTime, 78),
		},
		{
			name:   "single high measurement does not trigger",
			window: []model.TelemetryEvent{testEvent("event-001", baseTime, HighHeartRateThreshold+1)},
			latest: testEvent("event-001", baseTime, HighHeartRateThreshold+1),
		},
		{
			name: "threshold value does not trigger",
			window: []model.TelemetryEvent{
				testEvent("event-001", baseTime, HighHeartRateThreshold),
				testEvent("event-002", baseTime.Add(time.Minute), HighHeartRateThreshold),
			},
			latest: testEvent("event-002", baseTime.Add(time.Minute), HighHeartRateThreshold),
		},
		{
			name: "normal measurement inside minute suppresses alert",
			window: []model.TelemetryEvent{
				testEvent("event-001", baseTime, HighHeartRateThreshold+1),
				testEvent("event-002", baseTime.Add(30*time.Second), 80),
				latestHigh,
			},
			latest: latestHigh,
		},
		{
			name: "high heart rate sustained for a minute triggers alert",
			window: []model.TelemetryEvent{
				testEvent("event-001", baseTime, HighHeartRateThreshold+1),
				testEvent("event-002", baseTime.Add(30*time.Second), HighHeartRateThreshold+2),
				latestHigh,
			},
			latest:    latestHigh,
			wantAlert: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alert, ok := New().Detect(tt.window, tt.latest)
			if ok != tt.wantAlert {
				t.Fatalf("Detect() alert = %v, want %v", ok, tt.wantAlert)
			}
			if !ok {
				return
			}
			if alert.PatientID != tt.latest.PatientID {
				t.Fatalf("alert patient_id = %q, want %q", alert.PatientID, tt.latest.PatientID)
			}
			if alert.Type != model.AlertTypeHighHeartRate {
				t.Fatalf("alert type = %q, want %q", alert.Type, model.AlertTypeHighHeartRate)
			}
			if alert.Message != model.HighHeartRateMessage {
				t.Fatalf("alert message = %q, want %q", alert.Message, model.HighHeartRateMessage)
			}
			if !alert.TriggeredAt.Equal(tt.latest.Timestamp) {
				t.Fatalf("alert triggered_at = %v, want %v", alert.TriggeredAt, tt.latest.Timestamp)
			}
		})
	}
}

func testEvent(eventID string, timestamp time.Time, heartRate int) model.TelemetryEvent {
	return model.TelemetryEvent{
		EventID:   eventID,
		DeviceID:  "device-001",
		PatientID: "patient-001",
		Timestamp: timestamp,
		HeartRate: heartRate,
	}
}
