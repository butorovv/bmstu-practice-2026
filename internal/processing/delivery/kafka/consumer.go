package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
	defaultDLQTopic     = "telemetry.dlq"
)

type ConsumerConfig struct {
	Brokers      []string
	Topic        string
	DLQTopic     string
	GroupID      string
	RetryBackoff time.Duration
}

type messageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

type Consumer struct {
	reader       messageReader
	dlqWriter    messageWriter
	handler      *MessageHandler
	logger       *log.Logger
	brokers      []string
	topic        string
	dlqTopic     string
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
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = defaultDLQTopic
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          cfg.Topic,
		MinBytes:       defaultMinBytes,
		MaxBytes:       defaultMaxBytes,
		CommitInterval: 0,
	})
	dlqWriter := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.Brokers...),
		Topic:                  cfg.DLQTopic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
	}

	return &Consumer{
		reader:       reader,
		dlqWriter:    dlqWriter,
		handler:      handler,
		logger:       logger,
		brokers:      cfg.Brokers,
		topic:        cfg.Topic,
		dlqTopic:     cfg.DLQTopic,
		groupID:      cfg.GroupID,
		retryBackoff: cfg.RetryBackoff,
	}
}

func newConsumerWithReader(
	reader messageReader,
	handler *MessageHandler,
	logger *log.Logger,
	retryBackoff time.Duration,
) *Consumer {
	if logger == nil {
		logger = log.Default()
	}
	if retryBackoff == 0 {
		retryBackoff = defaultRetryBackoff
	}

	return &Consumer{
		reader:       reader,
		handler:      handler,
		logger:       logger,
		retryBackoff: retryBackoff,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer func() {
		if err := c.Close(); err != nil && !isContextDone(ctx) {
			c.logger.Printf("failed to close kafka consumer topic=%s group_id=%s error=%v", c.topic, c.groupID, err)
		}
	}()

	for _, topic := range c.topicsToEnsure() {
		for {
			if err := c.ensureTopic(ctx, topic); err != nil {
				if isContextDone(ctx) {
					return nil
				}

				c.logger.Printf("failed to ensure kafka topic topic=%s error=%v", topic, err)
				if !sleepContext(ctx, c.retryBackoff) {
					return nil
				}
				continue
			}
			break
		}
	}

	c.logger.Printf(
		"starting kafka consumer brokers=%s topic=%s group_id=%s dlq_topic=%s",
		strings.Join(c.brokers, ","),
		c.topic,
		c.groupID,
		c.dlqTopic,
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
		if c.dlqWriter != nil {
			if err := c.dlqWriter.Close(); c.closeErr == nil {
				c.closeErr = err
			}
		}
	})

	return c.closeErr
}

func (c *Consumer) ensureTopic(ctx context.Context, topic string) error {
	if len(c.brokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	if topic == "" {
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
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
}

func (c *Consumer) topicsToEnsure() []string {
	topics := []string{c.topic}
	if c.dlqWriter != nil && c.dlqTopic != "" && c.dlqTopic != c.topic {
		topics = append(topics, c.dlqTopic)
	}

	return topics
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
			if err := c.sendToDLQWithRetry(ctx, message, err); err != nil {
				if isContextDone(ctx) {
					return nil
				}
				return err
			}
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
			if err := c.sendToDLQWithRetry(ctx, message, err); err != nil {
				if isContextDone(ctx) {
					return nil
				}
				return err
			}
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

func (c *Consumer) sendToDLQWithRetry(ctx context.Context, message kafkago.Message, reason error) error {
	for {
		if err := c.sendToDLQ(ctx, message, reason); err != nil {
			if isContextDone(ctx) {
				return ctx.Err()
			}

			c.logger.Printf(
				"failed to publish kafka message to dlq topic=%s source_topic=%s partition=%d offset=%d error=%v",
				c.dlqTopic,
				message.Topic,
				message.Partition,
				message.Offset,
				err,
			)
			if !sleepContext(ctx, c.retryBackoff) {
				return ctx.Err()
			}
			continue
		}

		return nil
	}
}

func (c *Consumer) sendToDLQ(ctx context.Context, message kafkago.Message, reason error) error {
	if c.dlqWriter == nil {
		return errors.New("kafka dlq writer is not configured")
	}

	payload, err := json.Marshal(deadLetterMessage{
		Reason:          reason.Error(),
		Timestamp:       time.Now().UTC(),
		SourceTopic:     message.Topic,
		SourcePartition: message.Partition,
		SourceOffset:    message.Offset,
		PayloadBase64:   base64.StdEncoding.EncodeToString(message.Value),
	})
	if err != nil {
		return err
	}

	return c.dlqWriter.WriteMessages(ctx, kafkago.Message{
		Key:   message.Key,
		Value: payload,
		Time:  time.Now().UTC(),
		Headers: []kafkago.Header{
			{Key: "source_topic", Value: []byte(message.Topic)},
			{Key: "source_partition", Value: []byte(strconv.Itoa(message.Partition))},
			{Key: "source_offset", Value: []byte(strconv.FormatInt(message.Offset, 10))},
			{Key: "reason", Value: []byte(reason.Error())},
		},
	})
}

type deadLetterMessage struct {
	Reason          string    `json:"reason"`
	Timestamp       time.Time `json:"timestamp"`
	SourceTopic     string    `json:"source_topic"`
	SourcePartition int       `json:"source_partition"`
	SourceOffset    int64     `json:"source_offset"`
	PayloadBase64   string    `json:"payload_base64"`
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
