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

// PlayerBonusProcessor handles messages from bonus.v1.playerBonus topic
type PlayerBonusProcessor struct {
	env        string
	repo       *database.Repository
	logger     *zap.Logger
	errorCount atomic.Int64
}

func NewPlayerBonusProcessor(env string, repo *database.Repository, logger *zap.Logger) *PlayerBonusProcessor {
	return &PlayerBonusProcessor{
		env:    env,
		repo:   repo,
		logger: logger,
	}
}

func (p *PlayerBonusProcessor) ProcessMessage(ctx context.Context, msg *kafka.Message) ProcessingResult {
	p.logger.Debug("processing player bonus message",
		zap.String("topic", msg.Topic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.String("key", string(msg.Key)),
	)

	// Stage 1: Parse and validate schema
	playerBonusMsg, err := models.ParsePlayerBonusMessage(msg.Value)
	if err != nil {
		return p.handleSchemaError(ctx, msg, fmt.Sprintf("JSON parsing failed: %v", err))
	}

	// Check for required fields
	if playerBonusMsg.Message.EventType == "" {
		return p.handleSchemaError(ctx, msg, "missing message.event_type")
	}
	if playerBonusMsg.PlayerBonus.ID == "" {
		return p.handleSchemaError(ctx, msg, "missing player_bonus.id")
	}
	if playerBonusMsg.PlayerBonus.PlayerID == "" {
		return p.handleSchemaError(ctx, msg, "missing player_bonus.player_id")
	}
	if playerBonusMsg.PlayerBonus.BonusID == "" {
		return p.handleSchemaError(ctx, msg, "missing player_bonus.bonus_id")
	}
	if playerBonusMsg.PlayerBonus.Node == "" {
		return p.handleSchemaError(ctx, msg, "missing player_bonus.node")
	}

	// Stage 2: Convert to database model
	event, err := playerBonusMsg.ToPlayerBonusEvent(p.env, msg.Topic, msg.Partition, msg.Offset, msg.Value)
	if err != nil {
		return p.handleSchemaError(ctx, msg, fmt.Sprintf("validation conversion failed: %v", err))
	}

	// Stage 3: Insert into database
	insertedID, err := p.repo.InsertPlayerBonusEvent(ctx, event)
	if err != nil {
		p.errorCount.Add(1)
		p.logger.Error("failed to insert player bonus event",
			zap.Error(err),
			zap.String("env", p.env),
			zap.String("player_bonus_id", event.PlayerBonusID.String()),
			zap.Int64("offset", event.KafkaOffset),
		)
		return ProcessingResult{
			Type:    ResultInfraError,
			Message: "failed to insert player bonus event",
			Err:     err,
		}
	}

	if insertedID == 0 {
		// Technical duplicate by offset
		return ProcessingResult{
			Type:    ResultDuplicateOffset,
			Message: "duplicate offset",
		}
	}

	// Success
	return ProcessingResult{
		Type:    ResultOK,
		Message: "player bonus event processed successfully",
	}
}

// handleSchemaError processes schema/parsing errors
func (p *PlayerBonusProcessor) handleSchemaError(ctx context.Context, msg *kafka.Message, schemaError string) ProcessingResult {
	p.logger.Warn("player bonus schema validation failed",
		zap.String("error", schemaError),
		zap.String("env", p.env),
		zap.String("topic", msg.Topic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
	)

	insertedID, err := p.repo.InsertPlayerBonusEventWithError(
		ctx,
		p.env,
		msg.Topic,
		msg.Partition,
		msg.Offset,
		msg.Value,
		schemaError,
	)

	if err != nil {
		p.errorCount.Add(1)
		p.logger.Error("failed to insert player bonus schema error event",
			zap.Error(err),
			zap.String("env", p.env),
			zap.Int64("offset", msg.Offset),
		)
		return ProcessingResult{
			Type:    ResultInfraError,
			Message: "failed to insert schema error",
			Err:     err,
		}
	}

	if insertedID == 0 {
		// Technical duplicate by offset
		return ProcessingResult{
			Type:    ResultDuplicateOffset,
			Message: "duplicate offset (schema error already recorded)",
		}
	}

	// Successfully recorded as domain error
	return ProcessingResult{
		Type:    ResultDomainError,
		Message: fmt.Sprintf("schema_error: %s", schemaError),
	}
}

func (p *PlayerBonusProcessor) GetErrorCount() int64 {
	return p.errorCount.Load()
}
