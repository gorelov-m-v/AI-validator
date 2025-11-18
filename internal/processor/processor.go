package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"

	"AI-validator/internal/database"
	"AI-validator/internal/kafka"
	"AI-validator/internal/models"

	"go.uber.org/zap"
)

type Processor struct {
	env        string
	db         *database.DB
	repo       *database.Repository
	logger     *zap.Logger
	errorCount atomic.Int64
}

func New(env string, db *database.DB, repo *database.Repository, logger *zap.Logger) *Processor {
	return &Processor{
		env:    env,
		db:     db,
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

	if err := p.processEventWithSequence(ctx, event); err != nil {
		p.errorCount.Add(1)
		return fmt.Errorf("failed to process event with sequence: %w", err)
	}

	return nil
}

func (p *Processor) processEventWithSequence(ctx context.Context, event *models.BonusEventValidation) error {
	tx, err := p.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	txRepo := database.NewTxRepository(tx, p.logger)

	seqKey := event.SeqKey.String()
	prevState, err := txRepo.GetSequenceStateForUpdate(ctx, p.env, seqKey)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to get sequence state: %w", err)
	}

	if prevState != nil {
		event.PrevEventType = prevState.LastEventType
		event.PrevPlayerBonusStatus = prevState.LastPlayerBonusStatus
		event.PrevBalance = prevState.LastBalance
		event.PrevWager = prevState.LastWager
		event.PrevTotalWager = prevState.LastTotalWager
		event.PrevEventTS = sql.NullTime{Time: prevState.LastEventTS, Valid: true}
		event.PrevKafkaOffset = sql.NullInt64{Int64: prevState.LastKafkaOffset, Valid: true}

		sequenceOK, sequenceReason := p.checkSequence(event, prevState)
		event.SequenceOK = sequenceOK
		if sequenceReason != "" {
			event.SequenceReason = sql.NullString{String: sequenceReason, Valid: true}
		}
	} else {
		event.SequenceOK = true
	}

	insertedID, err := txRepo.InsertBonusEvent(ctx, event)
	if err != nil {
		return fmt.Errorf("failed to insert bonus event: %w", err)
	}

	if insertedID == 0 {
		return tx.Commit()
	}

	eventTS := event.ProcessedAt
	if event.EventTS.Valid {
		eventTS = event.EventTS.Time
	}

	newState := &models.BonusSequenceState{
		Env:                   p.env,
		SeqKey:                event.SeqKey,
		LastEventTS:           eventTS,
		LastKafkaTopic:        event.KafkaTopic,
		LastKafkaPartition:    event.KafkaPartition,
		LastKafkaOffset:       event.KafkaOffset,
		LastEventType:         sql.NullString{String: event.EventType, Valid: true},
		LastPlayerBonusStatus: event.PlayerBonusStatus,
		LastBalance:           event.Balance,
		LastWager:             event.Wager,
		LastTotalWager:        event.TotalWager,
		LastValidationID:      sql.NullInt64{Int64: insertedID, Valid: true},
	}

	if err := txRepo.UpsertSequenceState(ctx, newState); err != nil {
		return fmt.Errorf("failed to upsert sequence state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (p *Processor) checkSequence(event *models.BonusEventValidation, prevState *models.BonusSequenceState) (bool, string) {
	var reasons []string

	if event.EventTS.Valid {
		if event.EventTS.Time.Before(prevState.LastEventTS) {
			reasons = append(reasons, "event_ts moved backwards")
		}
	}

	if event.KafkaTopic == prevState.LastKafkaTopic && event.KafkaPartition == prevState.LastKafkaPartition {
		if event.KafkaOffset <= prevState.LastKafkaOffset {
			reasons = append(reasons, "offset moved backwards")
		}
	}

	if len(reasons) > 0 {
		return false, strings.Join(reasons, "; ")
	}

	return true, ""
}

func (p *Processor) GetErrorCount() int64 {
	return p.errorCount.Load()
}
