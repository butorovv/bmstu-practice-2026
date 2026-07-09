package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeMessageWriter struct {
	err      error
	messages []kafka.Message
	closed   bool
}

func (f *fakeMessageWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
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

func TestNewKafkaPublisherUsesContractSettings(t *testing.T) {
	pub, err := NewKafkaPublisher([]string{"localhost:9092"}, time.Second)
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
	if writer.Async {
		t.Fatal("writer must publish synchronously")
	}
	if _, ok := writer.Balancer.(*kafka.Hash); !ok {
		t.Fatalf("balancer type = %T, want *kafka.Hash", writer.Balancer)
	}
	if writer.AllowAutoTopicCreation {
		t.Fatal("automatic topic creation must be disabled")
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
