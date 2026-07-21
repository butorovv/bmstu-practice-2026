package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	DefaultKafkaMaxAttempts = 5
	DefaultKafkaMaxInFlight = 32
)

var ErrBackpressure = errors.New("publisher backpressure")

type messageWriter interface {
	WriteMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

type KafkaPublisherConfig struct {
	Brokers        []string
	PublishTimeout time.Duration
	MaxAttempts    int
	MaxInFlight    int
}

type KafkaPublisher struct {
	writer         messageWriter
	publishTimeout time.Duration
	inFlight       chan struct{}
	readinessCheck func(context.Context) error
}

func NewKafkaPublisher(brokers []string, publishTimeout time.Duration) (*KafkaPublisher, error) {
	return NewKafkaPublisherWithConfig(KafkaPublisherConfig{
		Brokers:        brokers,
		PublishTimeout: publishTimeout,
		MaxAttempts:    DefaultKafkaMaxAttempts,
		MaxInFlight:    DefaultKafkaMaxInFlight,
	})
}

func NewKafkaPublisherWithConfig(cfg KafkaPublisherConfig) (*KafkaPublisher, error) {
	brokers := cfg.Brokers
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	publishTimeout := cfg.PublishTimeout
	if publishTimeout <= 0 {
		return nil, errors.New("Kafka publish timeout must be positive")
	}
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultKafkaMaxAttempts
	}
	maxInFlight := cfg.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = DefaultKafkaMaxInFlight
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  TelemetryRawTopic,
		Balancer:               &kafka.Hash{},
		MaxAttempts:            maxAttempts,
		WriteBackoffMin:        100 * time.Millisecond,
		WriteBackoffMax:        time.Second,
		BatchTimeout:           10 * time.Millisecond,
		ReadTimeout:            publishTimeout,
		WriteTimeout:           publishTimeout,
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
	}

	publisher := newKafkaPublisher(writer, publishTimeout, maxInFlight)
	publisher.readinessCheck = kafkaReadinessCheck(brokers)
	return publisher, nil
}

func newKafkaPublisher(writer messageWriter, publishTimeout time.Duration, maxInFlight ...int) *KafkaPublisher {
	limit := DefaultKafkaMaxInFlight
	if len(maxInFlight) > 0 && maxInFlight[0] > 0 {
		limit = maxInFlight[0]
	}

	return &KafkaPublisher{
		writer:         writer,
		publishTimeout: publishTimeout,
		inFlight:       make(chan struct{}, limit),
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event TelemetryEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	publishCtx, cancel := context.WithTimeout(ctx, p.publishTimeout)
	defer cancel()

	if err := p.acquire(publishCtx); err != nil {
		return err
	}
	defer p.release()

	return p.writer.WriteMessages(publishCtx, kafka.Message{
		Key:   []byte(event.PatientID),
		Value: payload,
		Time:  event.Timestamp,
	})
}

func (p *KafkaPublisher) acquire(ctx context.Context) error {
	select {
	case p.inFlight <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", ErrBackpressure, ctx.Err())
	}
}

func (p *KafkaPublisher) release() {
	<-p.inFlight
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

func (p *KafkaPublisher) Ready(ctx context.Context) error {
	if p.readinessCheck == nil {
		return nil
	}

	return p.readinessCheck(ctx)
}

func kafkaReadinessCheck(brokers []string) func(context.Context) error {
	return func(ctx context.Context) error {
		var dialErrors []error
		for _, broker := range brokers {
			conn, err := (&kafka.Dialer{}).DialLeader(
				ctx,
				"tcp",
				broker,
				TelemetryRawTopic,
				0,
			)
			if err != nil {
				dialErrors = append(dialErrors, fmt.Errorf("broker %s: %w", broker, err))
				continue
			}

			if err := conn.Close(); err != nil {
				return fmt.Errorf("close Kafka readiness connection: %w", err)
			}
			return nil
		}

		return fmt.Errorf(
			"Kafka topic %s is unavailable: %w",
			TelemetryRawTopic,
			errors.Join(dialErrors...),
		)
	}
}
