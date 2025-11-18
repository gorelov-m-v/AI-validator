package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Consumer struct {
	reader *kafka.Reader
	logger *zap.Logger
}

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time
}

func NewConsumer(brokers []string, topic, groupID string, logger *zap.Logger) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("brokers list is empty")
	}
	if topic == "" {
		return nil, fmt.Errorf("topic is empty")
	}
	if groupID == "" {
		return nil, fmt.Errorf("groupID is empty")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,          // 1 byte
		MaxBytes:       10e6,       // 10MB
		CommitInterval: time.Second, // Auto-commit every second
		StartOffset:    kafka.LastOffset,
		Logger:         kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Debug(fmt.Sprintf(msg, args...))
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Error(fmt.Sprintf(msg, args...))
		}),
	})

	logger.Info("kafka consumer created",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group_id", groupID),
	)

	return &Consumer{
		reader: reader,
		logger: logger,
	}, nil
}

func (c *Consumer) ReadMessage(ctx context.Context) (*Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch message: %w", err)
	}

	return &Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Time:      msg.Time,
	}, nil
}

func (c *Consumer) CommitMessage(ctx context.Context, msg *Message) error {
	kafkaMsg := kafka.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Time:      msg.Time,
	}

	if err := c.reader.CommitMessages(ctx, kafkaMsg); err != nil {
		return fmt.Errorf("failed to commit message: %w", err)
	}

	return nil
}

func (c *Consumer) Close() error {
	c.logger.Info("closing kafka consumer")
	return c.reader.Close()
}
