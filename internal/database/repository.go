package database

import (
	"context"
	"database/sql"
	"fmt"

	"AI-validator/internal/models"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type RepositoryOperations interface {
	InsertBonusEvent(ctx context.Context, event *models.BonusEventValidation) (int64, error)
	GetSequenceStateForUpdate(ctx context.Context, env string, seqKey string) (*models.BonusSequenceState, error)
	UpsertSequenceState(ctx context.Context, state *models.BonusSequenceState) error
}

type repositoryImpl struct {
	ext    sqlx.ExtContext
	logger *zap.Logger
}

func newRepositoryImpl(ext sqlx.ExtContext, logger *zap.Logger) *repositoryImpl {
	return &repositoryImpl{
		ext:    ext,
		logger: logger,
	}
}

type Repository struct {
	db     *DB
	logger *zap.Logger
	*repositoryImpl
}

func NewRepository(db *DB, logger *zap.Logger) *Repository {
	return &Repository{
		db:             db,
		logger:         logger,
		repositoryImpl: newRepositoryImpl(db.conn, logger),
	}
}

func (r *repositoryImpl) InsertBonusEvent(ctx context.Context, event *models.BonusEventValidation) (int64, error) {
	query := `
		INSERT INTO bonus_info_bonus_events (
			env, kafka_topic, kafka_partition, kafka_offset,
			processed_at, event_ts,
			seq_key, player_id, node_id, bonus_id,
			event_type, bonus_category, currency,
			received_balance, balance, wager, total_wager,
			player_bonus_status, expired_at,
			created_at, updated_at, is_wager,
			prev_event_type, prev_player_bonus_status, prev_balance, prev_wager, prev_total_wager,
			prev_event_ts, prev_kafka_offset,
			schema_ok, schema_error,
			sequence_ok, sequence_reason,
			raw_message
		) VALUES (
			:env, :kafka_topic, :kafka_partition, :kafka_offset,
			:processed_at, :event_ts,
			:seq_key, :player_id, :node_id, :bonus_id,
			:event_type, :bonus_category, :currency,
			:received_balance, :balance, :wager, :total_wager,
			:player_bonus_status, :expired_at,
			:created_at, :updated_at, :is_wager,
			:prev_event_type, :prev_player_bonus_status, :prev_balance, :prev_wager, :prev_total_wager,
			:prev_event_ts, :prev_kafka_offset,
			:schema_ok, :schema_error,
			:sequence_ok, :sequence_reason,
			:raw_message
		)
		ON CONFLICT (env, kafka_topic, kafka_partition, kafka_offset) DO NOTHING
		RETURNING id
	`

	rows, err := sqlx.NamedQueryContext(ctx, r.ext, query, event)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			r.logger.Error("postgres error",
				zap.String("code", pgErr.Code),
				zap.String("message", pgErr.Message),
				zap.String("detail", pgErr.Detail),
			)
		}
		return 0, fmt.Errorf("failed to insert bonus event: %w", err)
	}
	defer rows.Close()

	var insertedID int64
	if rows.Next() {
		if err := rows.Scan(&insertedID); err != nil {
			return 0, fmt.Errorf("failed to scan inserted ID: %w", err)
		}

		r.logger.Debug("event inserted",
			zap.String("env", event.Env),
			zap.String("seq_key", event.SeqKey.String()),
			zap.String("event_type", event.EventType),
			zap.Int64("offset", event.KafkaOffset),
			zap.Int64("id", insertedID),
		)

		return insertedID, nil
	}

	r.logger.Debug("duplicate event skipped",
		zap.String("env", event.Env),
		zap.String("topic", event.KafkaTopic),
		zap.Int("partition", event.KafkaPartition),
		zap.Int64("offset", event.KafkaOffset),
	)

	return 0, nil
}

func (r *Repository) InsertBonusEventWithError(ctx context.Context, env, topic string, partition int, offset int64, rawJSON []byte, schemaError string) (int64, error) {
	query := `
		INSERT INTO bonus_info_bonus_events (
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
		RETURNING id
	`

	rows, err := r.db.conn.QueryxContext(ctx, query, env, topic, partition, offset, schemaError, rawJSON)
	if err != nil {
		return 0, fmt.Errorf("failed to insert error event: %w", err)
	}
	defer rows.Close()

	var insertedID int64
	if rows.Next() {
		if err := rows.Scan(&insertedID); err != nil {
			return 0, fmt.Errorf("failed to scan inserted ID: %w", err)
		}

		r.logger.Warn("invalid event inserted",
			zap.String("env", env),
			zap.String("topic", topic),
			zap.Int("partition", partition),
			zap.Int64("offset", offset),
			zap.Int64("id", insertedID),
			zap.String("error", schemaError),
		)

		return insertedID, nil
	}

	r.logger.Debug("duplicate error event skipped",
		zap.String("env", env),
		zap.String("topic", topic),
		zap.Int("partition", partition),
		zap.Int64("offset", offset),
	)

	return 0, nil
}

func (r *repositoryImpl) GetSequenceStateForUpdate(ctx context.Context, env string, seqKey string) (*models.BonusSequenceState, error) {
	query := `
		SELECT env, seq_key, last_event_ts, last_kafka_topic, last_kafka_partition, last_kafka_offset,
		       last_event_type, last_player_bonus_status, last_balance, last_wager, last_total_wager,
		       last_validation_id
		FROM bonus_info_bonus_sequence
		WHERE env = $1 AND seq_key = $2
		FOR UPDATE
	`

	var state models.BonusSequenceState
	err := sqlx.GetContext(ctx, r.ext, &state, query, env, seqKey)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Debug("sequence state not found (first event for this key)",
				zap.String("env", env),
				zap.String("seq_key", seqKey),
			)
		}
		return nil, err
	}

	return &state, nil
}

func (r *repositoryImpl) UpsertSequenceState(ctx context.Context, state *models.BonusSequenceState) error {
	query := `
		INSERT INTO bonus_info_bonus_sequence (
			env, seq_key, last_event_ts, last_kafka_topic, last_kafka_partition, last_kafka_offset,
			last_event_type, last_player_bonus_status, last_balance, last_wager, last_total_wager,
			last_validation_id
		) VALUES (
			:env, :seq_key, :last_event_ts, :last_kafka_topic, :last_kafka_partition, :last_kafka_offset,
			:last_event_type, :last_player_bonus_status, :last_balance, :last_wager, :last_total_wager,
			:last_validation_id
		)
		ON CONFLICT (env, seq_key) DO UPDATE SET
			last_event_ts = EXCLUDED.last_event_ts,
			last_kafka_topic = EXCLUDED.last_kafka_topic,
			last_kafka_partition = EXCLUDED.last_kafka_partition,
			last_kafka_offset = EXCLUDED.last_kafka_offset,
			last_event_type = EXCLUDED.last_event_type,
			last_player_bonus_status = EXCLUDED.last_player_bonus_status,
			last_balance = EXCLUDED.last_balance,
			last_wager = EXCLUDED.last_wager,
			last_total_wager = EXCLUDED.last_total_wager,
			last_validation_id = EXCLUDED.last_validation_id
	`

	_, err := sqlx.NamedExecContext(ctx, r.ext, query, state)
	if err != nil {
		return fmt.Errorf("failed to upsert sequence state: %w", err)
	}

	return nil
}

type TxRepository struct {
	*repositoryImpl
}

func NewTxRepository(tx *sqlx.Tx, logger *zap.Logger) *TxRepository {
	return &TxRepository{
		repositoryImpl: newRepositoryImpl(tx, logger),
	}
}

func (r *Repository) InsertPlayerBonusEvent(ctx context.Context, event *models.PlayerBonusEvent) (int64, error) {
	query := `
		INSERT INTO bonus_player_bonus_events (
			env, kafka_topic, kafka_partition, kafka_offset,
			processed_at,
			player_bonus_id, player_id, bonus_id, bonus_category, currency, node_id,
			balance, wager,
			processing_transfer_type, processing_transfer_value,
			processing_real_percent, processing_bonus_percent,
			bet_min, bet_max, threshold,
			event_type,
			schema_ok, schema_error,
			raw_message
		) VALUES (
			:env, :kafka_topic, :kafka_partition, :kafka_offset,
			:processed_at,
			:player_bonus_id, :player_id, :bonus_id, :bonus_category, :currency, :node_id,
			:balance, :wager,
			:processing_transfer_type, :processing_transfer_value,
			:processing_real_percent, :processing_bonus_percent,
			:bet_min, :bet_max, :threshold,
			:event_type,
			:schema_ok, :schema_error,
			:raw_message
		)
		ON CONFLICT (env, kafka_topic, kafka_partition, kafka_offset) DO NOTHING
		RETURNING id
	`

	rows, err := r.db.conn.NamedQueryContext(ctx, query, event)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			r.logger.Error("postgres error",
				zap.String("code", pgErr.Code),
				zap.String("message", pgErr.Message),
				zap.String("detail", pgErr.Detail),
			)
		}
		return 0, fmt.Errorf("failed to insert player bonus event: %w", err)
	}
	defer rows.Close()

	var insertedID int64
	if rows.Next() {
		if err := rows.Scan(&insertedID); err != nil {
			return 0, fmt.Errorf("failed to scan inserted ID: %w", err)
		}

		r.logger.Debug("player bonus event inserted",
			zap.String("env", event.Env),
			zap.String("player_bonus_id", event.PlayerBonusID.String()),
			zap.String("event_type", event.EventType),
			zap.Int64("offset", event.KafkaOffset),
			zap.Int64("id", insertedID),
		)

		return insertedID, nil
	}

	r.logger.Debug("duplicate player bonus event skipped",
		zap.String("env", event.Env),
		zap.String("topic", event.KafkaTopic),
		zap.Int("partition", event.KafkaPartition),
		zap.Int64("offset", event.KafkaOffset),
	)

	return 0, nil
}

func (r *Repository) InsertPlayerBonusEventWithError(ctx context.Context, env, topic string, partition int, offset int64, rawJSON []byte, schemaError string) (int64, error) {
	query := `
		INSERT INTO bonus_player_bonus_events (
			env, kafka_topic, kafka_partition, kafka_offset,
			processed_at,
			player_bonus_id, player_id, bonus_id, node_id,
			event_type,
			schema_ok, schema_error,
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
			$6
		)
		ON CONFLICT (env, kafka_topic, kafka_partition, kafka_offset) DO NOTHING
		RETURNING id
	`

	rows, err := r.db.conn.QueryxContext(ctx, query, env, topic, partition, offset, schemaError, rawJSON)
	if err != nil {
		return 0, fmt.Errorf("failed to insert player bonus error event: %w", err)
	}
	defer rows.Close()

	var insertedID int64
	if rows.Next() {
		if err := rows.Scan(&insertedID); err != nil {
			return 0, fmt.Errorf("failed to scan inserted ID: %w", err)
		}

		r.logger.Warn("invalid player bonus event inserted",
			zap.String("env", env),
			zap.String("topic", topic),
			zap.Int("partition", partition),
			zap.Int64("offset", offset),
			zap.Int64("id", insertedID),
			zap.String("error", schemaError),
		)

		return insertedID, nil
	}

	r.logger.Debug("duplicate player bonus error event skipped",
		zap.String("env", env),
		zap.String("topic", topic),
		zap.Int("partition", partition),
		zap.Int64("offset", offset),
	)

	return 0, nil
}
