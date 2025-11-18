package processor

import (
	"context"
	"fmt"
	"sync/atomic"

	"AI-validator/internal/database"
	"AI-validator/internal/kafka"
	"AI-validator/internal/models"

	"go.uber.org/zap"
)

type Processor struct {
	env        string
	repo       *database.Repository
	logger     *zap.Logger
	errorCount atomic.Int64
}

func New(env string, repo *database.Repository, logger *zap.Logger) *Processor {
	return &Processor{
		env:    env,
		repo:   repo,
		logger: logger,
	}
}

func (p *Processor) ProcessMessage(ctx context.Context, msg *kafka.Message) error {
	p.logger.Debug("processing message",
		zap.String("topic", msg.Topic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.String("key", string(msg.Key)),
		zap.String("value", string(msg.Value)),
	)

	bonusMsg, err := models.ParseBonusMessage(msg.Value)
	if err != nil {
		p.logger.Warn("failed to parse bonus message",
			zap.Error(err),
			zap.Int64("offset", msg.Offset),
		)

		if insertErr := p.repo.InsertBonusEventWithError(
			ctx,
			p.env,
			msg.Topic,
			msg.Partition,
			msg.Offset,
			msg.Value,
			err.Error(),
		); insertErr != nil {
			p.errorCount.Add(1)
			return fmt.Errorf("failed to insert error event: %w", insertErr)
		}

		return nil
	}

	supportedTypes := map[string]bool{
		"playerBonusCreate": true,
		"playerBonusUpdate": true,
	}
	if !supportedTypes[bonusMsg.Message.EventType] {
		p.logger.Warn("unsupported eventType, skipping",
			zap.String("event_type", bonusMsg.Message.EventType),
			zap.String("topic", msg.Topic),
			zap.Int64("offset", msg.Offset),
		)
		return nil
	}

	event, err := bonusMsg.ToEventValidation(p.env, msg.Topic, msg.Partition, msg.Offset, msg.Value)
	if err != nil {
		p.logger.Warn("failed to convert to event validation",
			zap.Error(err),
			zap.Int64("offset", msg.Offset),
		)

		if insertErr := p.repo.InsertBonusEventWithError(
			ctx,
			p.env,
			msg.Topic,
			msg.Partition,
			msg.Offset,
			msg.Value,
			err.Error(),
		); insertErr != nil {
			p.errorCount.Add(1)
			return fmt.Errorf("failed to insert validation error event: %w", insertErr)
		}

		return nil
	}

	if err := p.repo.InsertBonusEvent(ctx, event); err != nil {
		p.errorCount.Add(1)
		return fmt.Errorf("failed to insert bonus event: %w", err)
	}

	return nil
}

func (p *Processor) GetErrorCount() int64 {
	return p.errorCount.Load()
}
