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

type ProcessingResultType int

const (
	ResultOK ProcessingResultType = iota
	ResultDomainError
	ResultDuplicateOffset
	ResultInfraError
)

var supportedTypes = map[string]bool{
	"playerBonusCreate": true,
	"playerBonusUpdate": true,
}

type ProcessingResult struct {
	Type    ProcessingResultType
	Message string
	Err     error
}

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

func (p *Processor) ProcessMessage(ctx context.Context, msg *kafka.Message) ProcessingResult {
	p.logger.Debug("processing message",
		zap.String("topic", msg.Topic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.String("key", string(msg.Key)),
	)

	bonusMsg, err := models.ParseBonusMessage(msg.Value)
	if err != nil {
		return p.handleSchemaError(ctx, msg, fmt.Sprintf("JSON parsing failed: %v", err))
	}

	if !supportedTypes[bonusMsg.Message.EventType] {
		p.logger.Debug("unsupported eventType, skipping",
			zap.String("event_type", bonusMsg.Message.EventType),
			zap.String("topic", msg.Topic),
			zap.Int64("offset", msg.Offset),
		)
		return ProcessingResult{Type: ResultOK, Message: "unsupported event type"}
	}

	event, err := bonusMsg.ToEventValidation(p.env, msg.Topic, msg.Partition, msg.Offset, msg.Value)
	if err != nil {
		return p.handleSchemaError(ctx, msg, fmt.Sprintf("validation conversion failed: %v", err))
	}

	return p.processEventWithSequence(ctx, event)
}

func (p *Processor) handleSchemaError(ctx context.Context, msg *kafka.Message, schemaError string) ProcessingResult {
	p.logger.Warn("schema validation failed",
		zap.String("error", schemaError),
		zap.String("env", p.env),
		zap.String("topic", msg.Topic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
	)

	insertedID, err := p.repo.InsertBonusEventWithError(
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
		p.logger.Error("failed to insert schema error event",
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
		return ProcessingResult{
			Type:    ResultDuplicateOffset,
			Message: "duplicate offset (schema error already recorded)",
		}
	}

	return ProcessingResult{
		Type:    ResultDomainError,
		Message: fmt.Sprintf("schema_error: %s", schemaError),
	}
}

func (p *Processor) processEventWithSequence(ctx context.Context, event *models.BonusEventValidation) ProcessingResult {
	tx, err := p.db.BeginTx(ctx)
	if err != nil {
		p.errorCount.Add(1)
		p.logger.Error("failed to begin transaction",
			zap.Error(err),
			zap.String("env", p.env),
			zap.String("seq_key", event.SeqKey.String()),
			zap.Int64("offset", event.KafkaOffset),
		)
		return ProcessingResult{
			Type:    ResultInfraError,
			Message: "failed to begin transaction",
			Err:     err,
		}
	}
	defer tx.Rollback()

	txRepo := database.NewTxRepository(tx, p.logger)

	seqKey := event.SeqKey.String()
	prevState, err := txRepo.GetSequenceStateForUpdate(ctx, p.env, seqKey)
	if err != nil && err != sql.ErrNoRows {
		p.errorCount.Add(1)
		p.logger.Error("failed to get sequence state",
			zap.Error(err),
			zap.String("env", p.env),
			zap.String("seq_key", seqKey),
			zap.Int64("offset", event.KafkaOffset),
		)
		return ProcessingResult{
			Type:    ResultInfraError,
			Message: "failed to get sequence state",
			Err:     err,
		}
	}

	isDomainError := false
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
			isDomainError = true
		}
	} else {
		var newStatus int
		if event.PlayerBonusStatus.Valid {
			newStatus = int(event.PlayerBonusStatus.Int32)
		}
		statusOK, statusReason := ValidateStatusTransition(0, newStatus, event.EventType)
		if !statusOK {
			event.SequenceOK = false
			event.SequenceReason = sql.NullString{String: statusReason, Valid: true}
			isDomainError = true
		} else {
			event.SequenceOK = true
		}
	}

	insertedID, err := txRepo.InsertBonusEvent(ctx, event)
	if err != nil {
		p.errorCount.Add(1)
		p.logger.Error("failed to insert bonus event",
			zap.Error(err),
			zap.String("env", p.env),
			zap.String("seq_key", seqKey),
			zap.Int64("offset", event.KafkaOffset),
		)
		return ProcessingResult{
			Type:    ResultInfraError,
			Message: "failed to insert bonus event",
			Err:     err,
		}
	}

	if insertedID == 0 {
		if err := tx.Commit(); err != nil {
			p.errorCount.Add(1)
			p.logger.Error("failed to commit transaction for duplicate",
				zap.Error(err),
				zap.String("env", p.env),
				zap.Int64("offset", event.KafkaOffset),
			)
			return ProcessingResult{
				Type:    ResultInfraError,
				Message: "failed to commit transaction for duplicate",
				Err:     err,
			}
		}

		return ProcessingResult{
			Type:    ResultDuplicateOffset,
			Message: "duplicate offset",
		}
	}

	if event.SchemaOK && event.SequenceOK {
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
			p.errorCount.Add(1)
			p.logger.Error("failed to upsert sequence state",
				zap.Error(err),
				zap.String("env", p.env),
				zap.String("seq_key", seqKey),
				zap.Int64("offset", event.KafkaOffset),
			)
			return ProcessingResult{
				Type:    ResultInfraError,
				Message: "failed to upsert sequence state",
				Err:     err,
			}
		}
	} else {
		p.logger.Warn("snapshot not updated due to validation failure",
			zap.String("env", p.env),
			zap.String("seq_key", seqKey),
			zap.Bool("schema_ok", event.SchemaOK),
			zap.Bool("sequence_ok", event.SequenceOK),
			zap.Int64("offset", event.KafkaOffset),
		)
	}

	if err := tx.Commit(); err != nil {
		p.errorCount.Add(1)
		p.logger.Error("failed to commit transaction",
			zap.Error(err),
			zap.String("env", p.env),
			zap.String("seq_key", seqKey),
			zap.Int64("offset", event.KafkaOffset),
		)
		return ProcessingResult{
			Type:    ResultInfraError,
			Message: "failed to commit transaction",
			Err:     err,
		}
	}

	if isDomainError {
		return ProcessingResult{
			Type:    ResultDomainError,
			Message: fmt.Sprintf("sequence_error: %s", event.SequenceReason.String),
		}
	}

	return ProcessingResult{
		Type:    ResultOK,
		Message: "event processed successfully",
	}
}

func (p *Processor) checkSequence(event *models.BonusEventValidation, prevState *models.BonusSequenceState) (bool, string) {
	var reasons []string

	if event.EventTS.Valid && !prevState.LastEventTS.IsZero() {
		if event.EventTS.Time.Before(prevState.LastEventTS) {
			reasons = append(reasons, "SEQUENCE_EVENT_TS_DECREASED")
		}
	}

	if event.KafkaTopic == prevState.LastKafkaTopic && event.KafkaPartition == prevState.LastKafkaPartition {
		if event.KafkaOffset < prevState.LastKafkaOffset {
			reasons = append(reasons, "SEQUENCE_OFFSET_DECREASED")
		}
	}

	var prevStatus int
	if prevState.LastPlayerBonusStatus.Valid {
		prevStatus = int(prevState.LastPlayerBonusStatus.Int32)
	}

	var newStatus int
	if event.PlayerBonusStatus.Valid {
		newStatus = int(event.PlayerBonusStatus.Int32)
	}

	statusOK, statusReason := ValidateStatusTransition(prevStatus, newStatus, event.EventType)
	if !statusOK {
		reasons = append(reasons, statusReason)
	}

	if len(reasons) > 0 {
		return false, strings.Join(reasons, "; ")
	}

	return true, ""
}

func (p *Processor) GetErrorCount() int64 {
	return p.errorCount.Load()
}
