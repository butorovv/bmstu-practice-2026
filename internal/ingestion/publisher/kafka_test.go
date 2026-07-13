package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeMessageWriter struct {
	err      error
	messages []kafka.Message
	closed   bool
	calls    int32
}

func (f *fakeMessageWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	atomic.AddInt32(&f.calls, 1)
	f.messages = append(f.messages, messages...)
	return f.err
}

func (f *fakeMessageWriter) Close() error {
	f.closed = true
	return nil
}

func TestKafkaPublisherPublishesContractMessage(t *testing.T) {
	writer := &fakeMessageWriter{}
	pub := newKafkaPublisher(writer, time.Second)
	event := TelemetryEvent{
		EventID:   "device-001-000001-0",
		DeviceID:  "device-001",
		PatientID: "patient-001",
		Timestamp: time.Date(2026, time.July, 7, 12, 0, 0, 0, time.UTC),
		HeartRate: 78,
	}

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("published messages = %d, want 1", len(writer.messages))
	}

	message := writer.messages[0]
	if string(message.Key) != event.PatientID {
		t.Fatalf("message key = %q, want %q", message.Key, event.PatientID)
	}
	if !message.Time.Equal(event.Timestamp) {
		t.Fatalf("message time = %v, want %v", message.Time, event.Timestamp)
	}

	var got TelemetryEvent
	if err := json.Unmarshal(message.Value, &got); err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if got != event {
		t.Fatalf("message event = %+v, want %+v", got, event)
	}
}

func TestKafkaPublisherReturnsWriterError(t *testing.T) {
	wantErr := errors.New("Kafka unavailable")
	pub := newKafkaPublisher(&fakeMessageWriter{err: wantErr}, time.Second)

	err := pub.Publish(context.Background(), TelemetryEvent{PatientID: "patient-001"})

	if !errors.Is(err, wantErr) {
		t.Fatalf("Publish() error = %v, want %v", err, wantErr)
	}
}

func TestKafkaPublisherReturnsBackpressureWhenInFlightLimitIsFull(t *testing.T) {
	writer := &fakeMessageWriter{}
	pub := newKafkaPublisher(writer, time.Millisecond, 1)
	pub.inFlight <- struct{}{}

	err := pub.Publish(context.Background(), TelemetryEvent{PatientID: "patient-001"})

	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrBackpressure)
	}
	if calls := atomic.LoadInt32(&writer.calls); calls != 0 {
		t.Fatalf("writer calls = %d, want 0", calls)
	}
}

func TestNewKafkaPublisherUsesContractSettings(t *testing.T) {
	pub, err := NewKafkaPublisherWithConfig(KafkaPublisherConfig{
		Brokers:        []string{"localhost:9092"},
		PublishTimeout: time.Second,
		MaxAttempts:    4,
		MaxInFlight:    7,
	})
	if err != nil {
		t.Fatalf("NewKafkaPublisher() error = %v", err)
	}
	defer pub.Close()

	writer, ok := pub.writer.(*kafka.Writer)
	if !ok {
		t.Fatalf("writer type = %T, want *kafka.Writer", pub.writer)
	}
	if writer.Topic != TelemetryRawTopic {
		t.Fatalf("topic = %q, want %q", writer.Topic, TelemetryRawTopic)
	}
	if writer.RequiredAcks != kafka.RequireAll {
		t.Fatalf("required acks = %v, want %v", writer.RequiredAcks, kafka.RequireAll)
	}
	if writer.MaxAttempts != 4 {
		t.Fatalf("max attempts = %d, want 4", writer.MaxAttempts)
	}
	if writer.WriteBackoffMin != 100*time.Millisecond {
		t.Fatalf("write backoff min = %v, want 100ms", writer.WriteBackoffMin)
	}
	if writer.WriteBackoffMax != time.Second {
		t.Fatalf("write backoff max = %v, want 1s", writer.WriteBackoffMax)
	}
	if writer.BatchTimeout != 10*time.Millisecond {
		t.Fatalf("batch timeout = %v, want 10ms", writer.BatchTimeout)
	}
	if writer.ReadTimeout != time.Second {
		t.Fatalf("read timeout = %v, want 1s", writer.ReadTimeout)
	}
	if writer.WriteTimeout != time.Second {
		t.Fatalf("write timeout = %v, want 1s", writer.WriteTimeout)
	}
	if writer.Async {
		t.Fatal("writer must publish synchronously")
	}
	if _, ok := writer.Balancer.(*kafka.Hash); !ok {
		t.Fatalf("balancer type = %T, want *kafka.Hash", writer.Balancer)
	}
	if writer.AllowAutoTopicCreation {
		t.Fatal("automatic topic creation must be disabled")
	}
	if cap(pub.inFlight) != 7 {
		t.Fatalf("in-flight limit = %d, want 7", cap(pub.inFlight))
	}
}

func TestKafkaPublisherCloseClosesWriter(t *testing.T) {
	writer := &fakeMessageWriter{}
	pub := newKafkaPublisher(writer, time.Second)

	if err := pub.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !writer.closed {
		t.Fatal("writer was not closed")
	}
}

func TestKafkaPublisherReadyUsesConfiguredCheck(t *testing.T) {
	wantErr := errors.New("Kafka unavailable")
	pub := newKafkaPublisher(&fakeMessageWriter{}, time.Second)
	pub.readinessCheck = func(context.Context) error {
		return wantErr
	}

	err := pub.Ready(context.Background())

	if !errors.Is(err, wantErr) {
		t.Fatalf("Ready() error = %v, want %v", err, wantErr)
	}
}
