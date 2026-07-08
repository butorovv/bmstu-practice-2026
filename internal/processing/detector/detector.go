package detector

import (
	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
)

const HighHeartRateThreshold = 120

type Detector struct{}

func New() Detector {
	return Detector{}
}

func (Detector) Detect(event model.TelemetryEvent) (model.Alert, bool) {
	if event.HeartRate <= HighHeartRateThreshold {
		return model.Alert{}, false
	}

	return model.Alert{
		PatientID:   event.PatientID,
		Type:        model.AlertTypeHighHeartRate,
		Message:     model.AlertMessageHighHeartRate,
		TriggeredAt: event.Timestamp,
	}, true
}
