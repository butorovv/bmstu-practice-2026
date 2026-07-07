package publisher

import (
	"context"
	"encoding/json"
	"log"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
)

const TelemetryRawTopic = "telemetry.raw"

type TelemetryEvent = usecase.TelemetryEvent

type Publisher interface {
	Publish(ctx context.Context, event TelemetryEvent) error
}

type LogPublisher struct{}

func NewLogPublisher() *LogPublisher {
	return &LogPublisher{}
}

func (LogPublisher) Publish(ctx context.Context, event TelemetryEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	log.Printf("published topic=%s key=%s event=%s", TelemetryRawTopic, event.PatientID, payload)
	return nil
}
