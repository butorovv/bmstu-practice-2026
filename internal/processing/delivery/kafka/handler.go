package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/validator"
)

var (
	ErrDecodeMessage         = errors.New("decode kafka message")
	ErrInvalidTelemetryEvent = errors.New("invalid telemetry event")
	ErrProcessTelemetryEvent = errors.New("process telemetry event")
)

type Processor interface {
	Process(ctx context.Context, event model.TelemetryEvent) (*usecase.ProcessingResult, error)
}

type MessageHandler struct {
	processor Processor
}

func NewMessageHandler(processor Processor) *MessageHandler {
	return &MessageHandler{
		processor: processor,
	}
}

func (h *MessageHandler) Handle(ctx context.Context, payload []byte) (*usecase.ProcessingResult, error) {
	var event model.TelemetryEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDecodeMessage, err)
	}

	result, err := h.processor.Process(ctx, event)
	if err != nil {
		if isValidationError(err) {
			return nil, fmt.Errorf("%w: %w", ErrInvalidTelemetryEvent, err)
		}

		return nil, fmt.Errorf("%w: %w", ErrProcessTelemetryEvent, err)
	}

	return result, nil
}

func isSkippableError(err error) bool {
	return errors.Is(err, ErrDecodeMessage) || errors.Is(err, ErrInvalidTelemetryEvent)
}

func isValidationError(err error) bool {
	return errors.Is(err, validator.ErrEmptyEventID) ||
		errors.Is(err, validator.ErrEmptyDeviceID) ||
		errors.Is(err, validator.ErrEmptyPatientID) ||
		errors.Is(err, validator.ErrEmptyTimestamp) ||
		errors.Is(err, validator.ErrTimestampNotUTC) ||
		errors.Is(err, validator.ErrHeartRateTooLow) ||
		errors.Is(err, validator.ErrHeartRateTooHigh)
}
