package kafka

import (
	"context"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const (
	defaultRetryBackoff = time.Second
	defaultMinBytes     = 1
	defaultMaxBytes     = 10e6
)

type ConsumerConfig struct {
	Brokers      []string
	Topic        string
	GroupID      string
	RetryBackoff time.Duration
}

type Consumer struct {
	reader       *kafkago.Reader
	handler      *MessageHandler
	logger       *log.Logger
	brokers      []string
	topic        string
	groupID      string
	retryBackoff time.Duration
	closeOnce    sync.Once
	closeErr     error
}

func NewConsumer(cfg ConsumerConfig, handler *MessageHandler, logger *log.Logger) *Consumer {
	if logger == nil {
		logger = log.Default()
	}
	if cfg.RetryBackoff == 0 {
		cfg.RetryBackoff = defaultRetryBackoff
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          cfg.Topic,
		MinBytes:       defaultMinBytes,
		MaxBytes:       defaultMaxBytes,
		CommitInterval: 0,
	})

	return &Consumer{
		reader:       reader,
		handler:      handler,
		logger:       logger,
		brokers:      cfg.Brokers,
		topic:        cfg.Topic,
		groupID:      cfg.GroupID,
		retryBackoff: cfg.RetryBackoff,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer func() {
		if err := c.Close(); err != nil && !isContextDone(ctx) {
			c.logger.Printf("failed to close kafka consumer topic=%s group_id=%s error=%v", c.topic, c.groupID, err)
		}
	}()

	for {
		if err := c.ensureTopic(ctx); err != nil {
			if isContextDone(ctx) {
				return nil
			}

			c.logger.Printf("failed to ensure kafka topic topic=%s error=%v", c.topic, err)
			if !sleepContext(ctx, c.retryBackoff) {
				return nil
			}
			continue
		}
		break
	}

	c.logger.Printf(
		"starting kafka consumer brokers=%s topic=%s group_id=%s",
		strings.Join(c.brokers, ","),
		c.topic,
		c.groupID,
	)

	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if isContextDone(ctx) {
				return nil
			}

			c.logger.Printf("failed to fetch kafka message topic=%s group_id=%s error=%v", c.topic, c.groupID, err)
			if !sleepContext(ctx, c.retryBackoff) {
				return nil
			}
			continue
		}

		if err := c.handleFetchedMessage(ctx, message); err != nil {
			return err
		}
	}
}

func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.reader.Close()
	})

	return c.closeErr
}

func (c *Consumer) ensureTopic(ctx context.Context) error {
	if len(c.brokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	if c.topic == "" {
		return errors.New("kafka topic is required")
	}

	conn, err := kafkago.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerAddress := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafkago.DialContext(ctx, "tcp", controllerAddress)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafkago.TopicConfig{
		Topic:             c.topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
}

func (c *Consumer) handleFetchedMessage(ctx context.Context, message kafkago.Message) error {
	for {
		result, err := c.handler.Handle(ctx, message.Value)
		if err == nil {
			c.logger.Printf(
				"telemetry event processed device_id=%s patient_id=%s event_id=%s topic=%s partition=%d offset=%d",
				result.Event.DeviceID,
				result.Event.PatientID,
				result.Event.EventID,
				message.Topic,
				message.Partition,
				message.Offset,
			)
			if result.AlertCreated && result.Alert != nil {
				c.logger.Printf(
					"high heart rate alert created patient_id=%s alert_type=%s",
					result.Alert.PatientID,
					result.Alert.Type,
				)
			}

			return c.commitWithRetry(ctx, message)
		}

		if isContextDone(ctx) {
			return nil
		}
		if errors.Is(err, ErrDecodeMessage) {
			c.logger.Printf(
				"failed to decode kafka message topic=%s partition=%d offset=%d error=%v",
				message.Topic,
				message.Partition,
				message.Offset,
				err,
			)
			return c.commitWithRetry(ctx, message)
		}
		if isSkippableError(err) {
			c.logger.Printf(
				"failed to process telemetry event topic=%s partition=%d offset=%d error=%v",
				message.Topic,
				message.Partition,
				message.Offset,
				err,
			)
			return c.commitWithRetry(ctx, message)
		}

		c.logger.Printf(
			"failed to process telemetry event topic=%s partition=%d offset=%d error=%v",
			message.Topic,
			message.Partition,
			message.Offset,
			err,
		)
		if !sleepContext(ctx, c.retryBackoff) {
			return nil
		}
	}
}

func (c *Consumer) commitWithRetry(ctx context.Context, message kafkago.Message) error {
	for {
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			if isContextDone(ctx) {
				return nil
			}

			c.logger.Printf(
				"failed to commit kafka message topic=%s partition=%d offset=%d error=%v",
				message.Topic,
				message.Partition,
				message.Offset,
				err,
			)
			if !sleepContext(ctx, c.retryBackoff) {
				return nil
			}
			continue
		}

		return nil
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func isContextDone(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded)
}
