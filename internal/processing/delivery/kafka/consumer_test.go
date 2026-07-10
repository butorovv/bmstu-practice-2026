package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/validator"
	kafkago "github.com/segmentio/kafka-go"
)

func TestConsumerDoesNotCommitOffsetWhenProcessingFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &fakeMessageReader{}
	writer := &fakeDLQWriter{}
	processor := &cancelingProcessor{
		cancel: cancel,
		err:    errors.New("postgres unavailable"),
	}
	consumer := newConsumerWithReader(
		reader,
		NewMessageHandler(processor),
		log.New(io.Discard, "", 0),
		time.Millisecond,
	)
	consumer.dlqWriter = writer
	consumer.dlqTopic = "telemetry.dlq"

	err := consumer.handleFetchedMessage(ctx, kafkago.Message{
		Topic:     "telemetry.raw",
		Partition: 0,
		Offset:    42,
		Value: []byte(`{
			"event_id": "event-001",
			"device_id": "device-001",
			"patient_id": "patient-001",
			"timestamp": "2026-07-07T12:00:00Z",
			"heart_rate": 78
		}`),
	})
	if err != nil {
		t.Fatalf("handleFetchedMessage() error = %v", err)
	}
	if processor.calls != 1 {
		t.Fatalf("processor calls = %d, want 1", processor.calls)
	}
	if reader.commitCalls != 0 {
		t.Fatalf("commit calls = %d, want 0", reader.commitCalls)
	}
	if writer.writeCalls != 0 {
		t.Fatalf("dlq write calls = %d, want 0", writer.writeCalls)
	}
}

func TestConsumerPublishesInvalidJSONToDLQBeforeCommit(t *testing.T) {
	reader := &fakeMessageReader{}
	writer := &fakeDLQWriter{}
	consumer := newConsumerWithReader(
		reader,
		NewMessageHandler(&cancelingProcessor{}),
		log.New(io.Discard, "", 0),
		time.Millisecond,
	)
	consumer.dlqWriter = writer
	consumer.dlqTopic = "telemetry.dlq"

	err := consumer.handleFetchedMessage(context.Background(), kafkago.Message{
		Topic:     "telemetry.raw",
		Partition: 2,
		Offset:    42,
		Value:     []byte(`{"invalid"`),
	})
	if err != nil {
		t.Fatalf("handleFetchedMessage() error = %v", err)
	}
	if writer.writeCalls != 1 {
		t.Fatalf("dlq write calls = %d, want 1", writer.writeCalls)
	}
	if reader.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1", reader.commitCalls)
	}

	var dlq deadLetterMessage
	if err := json.Unmarshal(writer.messages[0].Value, &dlq); err != nil {
		t.Fatalf("decode dlq message: %v", err)
	}
	if dlq.SourceTopic != "telemetry.raw" || dlq.SourcePartition != 2 || dlq.SourceOffset != 42 {
		t.Fatalf("dlq source metadata = %+v", dlq)
	}
	if dlq.Reason == "" || dlq.PayloadBase64 == "" {
		t.Fatalf("dlq diagnostic fields are empty: %+v", dlq)
	}
}

func TestConsumerDoesNotCommitOffsetWhenDLQPublishFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writer := &fakeDLQWriter{
		err:     errors.New("kafka unavailable"),
		onWrite: cancel,
	}
	consumer := newConsumerWithReader(
		&fakeMessageReader{},
		NewMessageHandler(&cancelingProcessor{}),
		log.New(io.Discard, "", 0),
		time.Millisecond,
	)
	consumer.dlqWriter = writer
	consumer.dlqTopic = "telemetry.dlq"

	err := consumer.handleFetchedMessage(ctx, kafkago.Message{
		Topic:     "telemetry.raw",
		Partition: 0,
		Offset:    7,
		Value:     []byte(`{"invalid"`),
	})
	if err != nil {
		t.Fatalf("handleFetchedMessage() error = %v", err)
	}
	if writer.writeCalls == 0 {
		t.Fatal("dlq writer was not called")
	}
	if consumer.reader.(*fakeMessageReader).commitCalls != 0 {
		t.Fatalf("commit calls = %d, want 0", consumer.reader.(*fakeMessageReader).commitCalls)
	}
}

func TestConsumerPublishesInvalidRequiredFieldToDLQBeforeCommit(t *testing.T) {
	reader := &fakeMessageReader{}
	writer := &fakeDLQWriter{}
	processor := &validationErrorProcessor{err: validator.ErrEmptyEventID}
	consumer := newConsumerWithReader(
		reader,
		NewMessageHandler(processor),
		log.New(io.Discard, "", 0),
		time.Millisecond,
	)
	consumer.dlqWriter = writer
	consumer.dlqTopic = "telemetry.dlq"

	err := consumer.handleFetchedMessage(context.Background(), kafkago.Message{
		Topic:     "telemetry.raw",
		Partition: 1,
		Offset:    10,
		Value: []byte(`{
			"device_id": "device-001",
			"patient_id": "patient-001",
			"timestamp": "2026-07-07T12:00:00Z",
			"heart_rate": 78
		}`),
	})
	if err != nil {
		t.Fatalf("handleFetchedMessage() error = %v", err)
	}
	if writer.writeCalls != 1 {
		t.Fatalf("dlq write calls = %d, want 1", writer.writeCalls)
	}
	if reader.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1", reader.commitCalls)
	}
}

type fakeMessageReader struct {
	commitCalls int
}

func (r *fakeMessageReader) FetchMessage(context.Context) (kafkago.Message, error) {
	return kafkago.Message{}, errors.New("not implemented")
}

func (r *fakeMessageReader) CommitMessages(context.Context, ...kafkago.Message) error {
	r.commitCalls++
	return nil
}

func (r *fakeMessageReader) Close() error {
	return nil
}

type fakeDLQWriter struct {
	messages   []kafkago.Message
	err        error
	onWrite    func()
	writeCalls int
}

func (w *fakeDLQWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.writeCalls++
	if w.onWrite != nil {
		w.onWrite()
	}
	if w.err != nil {
		return w.err
	}
	w.messages = append(w.messages, messages...)

	return nil
}

func (w *fakeDLQWriter) Close() error {
	return nil
}

type cancelingProcessor struct {
	cancel func()
	err    error
	calls  int
}

type validationErrorProcessor struct {
	err error
}

func (p *validationErrorProcessor) Process(
	_ context.Context,
	_ model.TelemetryEvent,
) (*usecase.ProcessingResult, error) {
	return nil, p.err
}

func (p *cancelingProcessor) Process(
	_ context.Context,
	_ model.TelemetryEvent,
) (*usecase.ProcessingResult, error) {
	p.calls++
	p.cancel()

	return nil, p.err
}
