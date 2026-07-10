package detector

import (
	"sort"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
)

const (
	HighHeartRateThreshold = 120
	HighHeartRateWindow    = time.Minute
)

type Detector struct{}

func New() Detector {
	return Detector{}
}

func (Detector) Detect(window []model.TelemetryEvent, latest model.TelemetryEvent) (model.Alert, bool) {
	if latest.HeartRate <= HighHeartRateThreshold || len(window) == 0 {
		return model.Alert{}, false
	}

	events := make([]model.TelemetryEvent, 0, len(window))
	for _, event := range window {
		if event.PatientID == latest.PatientID &&
			!event.Timestamp.Before(latest.Timestamp.Add(-HighHeartRateWindow)) &&
			!event.Timestamp.After(latest.Timestamp) {
			events = append(events, event)
		}
	}
	if len(events) == 0 {
		return model.Alert{}, false
	}

	sort.Slice(events, func(i int, j int) bool {
		return events[i].Timestamp.Before(events[j].Timestamp)
	})

	if latest.Timestamp.Sub(events[0].Timestamp) < HighHeartRateWindow {
		return model.Alert{}, false
	}

	for _, event := range events {
		if event.HeartRate <= HighHeartRateThreshold {
			return model.Alert{}, false
		}
	}

	return model.Alert{
		PatientID:   latest.PatientID,
		Type:        model.AlertTypeHighHeartRate,
		Message:     model.HighHeartRateMessage,
		TriggeredAt: latest.Timestamp,
	}, true
}
