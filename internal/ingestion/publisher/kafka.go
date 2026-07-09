package publisher

import (
	"context"
	"encoding/json"

	kafkago "github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
	writer *kafkago.Writer
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafkago.Writer{
			Addr:     kafkago.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafkago.Hash{},
		},
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event TelemetryEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(event.PatientID),
		Value: payload,
	})
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
