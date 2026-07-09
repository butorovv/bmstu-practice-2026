package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
)

type messageWriter interface {
	WriteMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

type KafkaPublisher struct {
	writer         messageWriter
	publishTimeout time.Duration
}

func NewKafkaPublisher(brokers []string, publishTimeout time.Duration) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if publishTimeout <= 0 {
		return nil, errors.New("Kafka publish timeout must be positive")
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  TelemetryRawTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
	}

	return newKafkaPublisher(writer, publishTimeout), nil
}

func newKafkaPublisher(writer messageWriter, publishTimeout time.Duration) *KafkaPublisher {
	return &KafkaPublisher{
		writer:         writer,
		publishTimeout: publishTimeout,
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event TelemetryEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	publishCtx, cancel := context.WithTimeout(ctx, p.publishTimeout)
	defer cancel()

	return p.writer.WriteMessages(publishCtx, kafka.Message{
		Key:   []byte(event.PatientID),
		Value: payload,
		Time:  event.Timestamp,
	})
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
