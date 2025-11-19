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
		MinBytes:       1,                // 1 byte
		MaxBytes:       10e6,             // 10MB
		StartOffset:    kafka.LastOffset, // Start from latest for new consumer groups
		CommitInterval: 0,                // Disable auto-commit, use manual batch commits
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
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

func (c *Consumer) ReadBatch(ctx context.Context, maxBatch int, maxWait time.Duration) ([]*Message, error) {
	batch := make([]*Message, 0, maxBatch)
	startTime := time.Now()

	firstMsg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch first message: %w", err)
	}

	batch = append(batch, &Message{
		Topic:     firstMsg.Topic,
		Partition: firstMsg.Partition,
		Offset:    firstMsg.Offset,
		Key:       firstMsg.Key,
		Value:     firstMsg.Value,
		Time:      firstMsg.Time,
	})

	for len(batch) < maxBatch {
		elapsed := time.Since(startTime)
		if elapsed >= maxWait {
			break
		}

		fetchCtx, cancel := context.WithTimeout(ctx, maxWait-elapsed)
		msg, err := c.reader.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			c.logger.Error("failed to fetch message in batch after first message",
				zap.Error(err),
				zap.Int("batch_size", len(batch)),
			)
			return batch, fmt.Errorf("failed to fetch message in batch: %w", err)
		}

		batch = append(batch, &Message{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
			Time:      msg.Time,
		})
	}

	return batch, nil
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

func (c *Consumer) CommitBatch(ctx context.Context, messages []*Message) error {
	if len(messages) == 0 {
		return nil
	}

	lastByPartition := make(map[int]*Message)
	for _, msg := range messages {
		if existing, ok := lastByPartition[msg.Partition]; !ok || msg.Offset > existing.Offset {
			lastByPartition[msg.Partition] = msg
		}
	}

	toCommit := make([]kafka.Message, 0, len(lastByPartition))
	for _, msg := range lastByPartition {
		toCommit = append(toCommit, kafka.Message{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
			Time:      msg.Time,
		})
	}

	if err := c.reader.CommitMessages(ctx, toCommit...); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	c.logger.Debug("committed batch offsets",
		zap.Int("partitions", len(lastByPartition)),
		zap.Int("total_messages", len(messages)),
	)

	return nil
}

func (c *Consumer) Close() error {
	c.logger.Warn("closing kafka consumer")
	return c.reader.Close()
}
