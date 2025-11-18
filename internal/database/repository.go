package database

import (
	"context"
	"fmt"

	"AI-validator/internal/models"

	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

type Repository struct {
	db     *DB
	logger *zap.Logger
}

func NewRepository(db *DB, logger *zap.Logger) *Repository {
	return &Repository{
		db:     db,
		logger: logger,
	}
}

func (r *Repository) InsertBonusEvent(ctx context.Context, event *models.BonusEventValidation) error {
	query := `
		INSERT INTO bonus_events_validation (
			env, kafka_topic, kafka_partition, kafka_offset,
			processed_at, event_ts,
			seq_key, player_id, node_id, bonus_id,
			event_type, bonus_category, currency,
			received_balance, balance, wager, total_wager,
			player_bonus_status, expired_at,
			created_at, updated_at, is_wager,
			schema_ok, schema_error,
			sequence_ok,
			raw_message
		) VALUES (
			:env, :kafka_topic, :kafka_partition, :kafka_offset,
			:processed_at, :event_ts,
			:seq_key, :player_id, :node_id, :bonus_id,
			:event_type, :bonus_category, :currency,
			:received_balance, :balance, :wager, :total_wager,
			:player_bonus_status, :expired_at,
			:created_at, :updated_at, :is_wager,
			:schema_ok, :schema_error,
			:sequence_ok,
			:raw_message
		)
		ON CONFLICT (env, kafka_topic, kafka_partition, kafka_offset) DO NOTHING
	`

	result, err := r.db.conn.NamedExecContext(ctx, query, event)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			r.logger.Error("postgres error",
				zap.String("code", pgErr.Code),
				zap.String("message", pgErr.Message),
				zap.String("detail", pgErr.Detail),
			)
		}
		return fmt.Errorf("failed to insert bonus event: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		r.logger.Debug("duplicate event skipped",
			zap.String("env", event.Env),
			zap.String("topic", event.KafkaTopic),
			zap.Int("partition", event.KafkaPartition),
			zap.Int64("offset", event.KafkaOffset),
		)
	} else {
		r.logger.Debug("event inserted",
			zap.String("env", event.Env),
			zap.String("seq_key", event.SeqKey.String()),
			zap.String("event_type", event.EventType),
			zap.Int64("offset", event.KafkaOffset),
		)
	}

	return nil
}

func (r *Repository) InsertBonusEventWithError(ctx context.Context, env, topic string, partition int, offset int64, rawJSON []byte, schemaError string) error {
	query := `
		INSERT INTO bonus_events_validation (
			env, kafka_topic, kafka_partition, kafka_offset,
			processed_at,
			seq_key, player_id, node_id, bonus_id,
			event_type,
			schema_ok, schema_error,
			sequence_ok,
			raw_message
		) VALUES (
			$1, $2, $3, $4,
			NOW(),
			'00000000-0000-0000-0000-000000000000',
			'00000000-0000-0000-0000-000000000000',
			'00000000-0000-0000-0000-000000000000',
			'00000000-0000-0000-0000-000000000000',
			'PARSE_ERROR',
			FALSE, $5,
			TRUE,
			$6
		)
		ON CONFLICT (env, kafka_topic, kafka_partition, kafka_offset) DO NOTHING
	`

	_, err := r.db.conn.ExecContext(ctx, query, env, topic, partition, offset, schemaError, rawJSON)
	if err != nil {
		return fmt.Errorf("failed to insert error event: %w", err)
	}

	r.logger.Warn("invalid event inserted",
		zap.String("env", env),
		zap.String("topic", topic),
		zap.Int("partition", partition),
		zap.Int64("offset", offset),
		zap.String("error", schemaError),
	)

	return nil
}
