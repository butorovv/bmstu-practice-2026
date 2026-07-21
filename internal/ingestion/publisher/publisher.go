package publisher

import (
	"context"
	"log/slog"

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

	slog.InfoContext(
		ctx,
		"telemetry event published",
		"topic", TelemetryRawTopic,
		"event_id", event.EventID,
		"device_id", event.DeviceID,
	)
	return nil
}
