package models

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type BonusMessage struct {
	Message struct {
		EventType string `json:"eventType"`
	} `json:"message"`
	Bonus struct {
		NodeID            string `json:"nodeId"`
		ID                string `json:"id"`
		BonusID           string `json:"bonusId"`
		BonusCategory     string `json:"bonusCategory"`
		PlayerID          string `json:"playerId"`
		Currency          string `json:"currency"`
		ReceivedBalance   string `json:"receivedBalance"`
		Balance           string `json:"balance"`
		Wager             string `json:"wager"`
		TotalWager        string `json:"totalWager"`
		PlayerBonusStatus int    `json:"playerBonusStatus"`
		ExpiredAt         string `json:"expiredAt"`
		UpdatedAt         int64  `json:"updatedAt"`
		CreatedAt         int64  `json:"createdAt"`
		IsWager           int    `json:"isWager"`
	} `json:"bonus"`
}

type BonusEventValidation struct {
	ID             int64        `db:"id"`
	Env            string       `db:"env"`
	KafkaTopic     string       `db:"kafka_topic"`
	KafkaPartition int          `db:"kafka_partition"`
	KafkaOffset    int64        `db:"kafka_offset"`
	ProcessedAt    time.Time    `db:"processed_at"`
	EventTS        sql.NullTime `db:"event_ts"`

	SeqKey   uuid.UUID `db:"seq_key"`
	PlayerID uuid.UUID `db:"player_id"`
	NodeID   uuid.UUID `db:"node_id"`
	BonusID  uuid.UUID `db:"bonus_id"`

	EventType         string              `db:"event_type"`
	BonusCategory     sql.NullString      `db:"bonus_category"`
	Currency          sql.NullString      `db:"currency"`
	ReceivedBalance   decimal.NullDecimal `db:"received_balance"`
	Balance           decimal.NullDecimal `db:"balance"`
	Wager             decimal.NullDecimal `db:"wager"`
	TotalWager        decimal.NullDecimal `db:"total_wager"`
	PlayerBonusStatus sql.NullInt32       `db:"player_bonus_status"`
	ExpiredAt         sql.NullInt64       `db:"expired_at"`
	CreatedAt         int64               `db:"created_at"`
	UpdatedAt         int64               `db:"updated_at"`
	IsWager           sql.NullBool        `db:"is_wager"`

	PrevEventType         sql.NullString      `db:"prev_event_type"`
	PrevPlayerBonusStatus sql.NullInt32       `db:"prev_player_bonus_status"`
	PrevBalance           decimal.NullDecimal `db:"prev_balance"`
	PrevWager             decimal.NullDecimal `db:"prev_wager"`
	PrevTotalWager        decimal.NullDecimal `db:"prev_total_wager"`
	PrevEventTS           sql.NullTime        `db:"prev_event_ts"`
	PrevKafkaOffset       sql.NullInt64       `db:"prev_kafka_offset"`

	SchemaOK    bool           `db:"schema_ok"`
	SchemaError sql.NullString `db:"schema_error"`

	ModelVersion  sql.NullString  `db:"model_version"`
	AnomalyScore  sql.NullFloat64 `db:"anomaly_score"`
	IsAnomaly     sql.NullBool    `db:"is_anomaly"`
	AnomalyReason sql.NullString  `db:"anomaly_reason"`

	SequenceOK     bool           `db:"sequence_ok"`
	SequenceReason sql.NullString `db:"sequence_reason"`

	Label        sql.NullString `db:"label"`
	LabelComment sql.NullString `db:"label_comment"`
	LabelUser    sql.NullString `db:"label_user"`
	LabelTS      sql.NullTime   `db:"label_ts"`

	RawMessage json.RawMessage `db:"raw_message"`
}

func ParseBonusMessage(data []byte) (*BonusMessage, error) {
	var msg BonusMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
	return &msg, nil
}

type BonusSequenceState struct {
	Env                   string              `db:"env"`
	SeqKey                uuid.UUID           `db:"seq_key"`
	LastEventTS           time.Time           `db:"last_event_ts"`
	LastKafkaTopic        string              `db:"last_kafka_topic"`
	LastKafkaPartition    int                 `db:"last_kafka_partition"`
	LastKafkaOffset       int64               `db:"last_kafka_offset"`
	LastEventType         sql.NullString      `db:"last_event_type"`
	LastPlayerBonusStatus sql.NullInt32       `db:"last_player_bonus_status"`
	LastBalance           decimal.NullDecimal `db:"last_balance"`
	LastWager             decimal.NullDecimal `db:"last_wager"`
	LastTotalWager        decimal.NullDecimal `db:"last_total_wager"`
	LastValidationID      sql.NullInt64       `db:"last_validation_id"`
}

func (bm *BonusMessage) ToEventValidation(env, topic string, partition int, offset int64, rawJSON []byte) (*BonusEventValidation, error) {
	ev := &BonusEventValidation{
		Env:            env,
		KafkaTopic:     topic,
		KafkaPartition: partition,
		KafkaOffset:    offset,
		ProcessedAt:    time.Now().UTC(),
		SchemaOK:       true,
		SequenceOK:     true,
		RawMessage:     rawJSON,
	}

	seqKey, err := uuid.Parse(bm.Bonus.ID)
	if err != nil {
		return nil, fmt.Errorf("invalid seq_key (bonus.id): %w", err)
	}
	ev.SeqKey = seqKey

	playerID, err := uuid.Parse(bm.Bonus.PlayerID)
	if err != nil {
		return nil, fmt.Errorf("invalid player_id: %w", err)
	}
	ev.PlayerID = playerID

	nodeID, err := uuid.Parse(bm.Bonus.NodeID)
	if err != nil {
		return nil, fmt.Errorf("invalid node_id: %w", err)
	}
	ev.NodeID = nodeID

	bonusID, err := uuid.Parse(bm.Bonus.BonusID)
	if err != nil {
		return nil, fmt.Errorf("invalid bonus_id: %w", err)
	}
	ev.BonusID = bonusID

	ev.EventType = bm.Message.EventType

	if bm.Bonus.BonusCategory != "" {
		ev.BonusCategory = sql.NullString{String: bm.Bonus.BonusCategory, Valid: true}
	}
	if bm.Bonus.Currency != "" {
		ev.Currency = sql.NullString{String: bm.Bonus.Currency, Valid: true}
	}

	if bm.Bonus.ReceivedBalance != "" {
		val, err := decimal.NewFromString(bm.Bonus.ReceivedBalance)
		if err != nil {
			return nil, fmt.Errorf("invalid received_balance: %w", err)
		}
		ev.ReceivedBalance = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	if bm.Bonus.Balance != "" {
		val, err := decimal.NewFromString(bm.Bonus.Balance)
		if err != nil {
			return nil, fmt.Errorf("invalid balance: %w", err)
		}
		ev.Balance = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	if bm.Bonus.Wager != "" {
		val, err := decimal.NewFromString(bm.Bonus.Wager)
		if err != nil {
			return nil, fmt.Errorf("invalid wager: %w", err)
		}
		ev.Wager = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	if bm.Bonus.TotalWager != "" {
		val, err := decimal.NewFromString(bm.Bonus.TotalWager)
		if err != nil {
			return nil, fmt.Errorf("invalid total_wager: %w", err)
		}
		ev.TotalWager = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	ev.PlayerBonusStatus = sql.NullInt32{Int32: int32(bm.Bonus.PlayerBonusStatus), Valid: true}

	if bm.Bonus.ExpiredAt != "" {
		var expiredAt int64
		_, err := fmt.Sscanf(bm.Bonus.ExpiredAt, "%d", &expiredAt)
		if err == nil {
			ev.ExpiredAt = sql.NullInt64{Int64: expiredAt, Valid: true}
		}
	}

	ev.CreatedAt = bm.Bonus.CreatedAt
	ev.UpdatedAt = bm.Bonus.UpdatedAt

	if bm.Bonus.UpdatedAt > 0 {
		ev.EventTS = sql.NullTime{Time: time.Unix(bm.Bonus.UpdatedAt, 0).UTC(), Valid: true}
	} else if bm.Bonus.CreatedAt > 0 {
		ev.EventTS = sql.NullTime{Time: time.Unix(bm.Bonus.CreatedAt, 0).UTC(), Valid: true}
	}

	ev.IsWager = sql.NullBool{Bool: bm.Bonus.IsWager == 1, Valid: true}

	return ev, nil
}

type PlayerBonusMessage struct {
	Message struct {
		EventType string `json:"event_type"`
	} `json:"message"`
	PlayerBonus struct {
		ID                      string  `json:"id"`
		PlayerID                string  `json:"player_id"`
		BonusID                 string  `json:"bonus_id"`
		BonusCategory           string  `json:"bonus_category"`
		Currency                string  `json:"currency"`
		Node                    string  `json:"node"`
		Balance                 string  `json:"balance"`
		Wager                   string  `json:"wager"`
		ProcessingTransferType  int     `json:"processing_transfer_type"`
		ProcessingTransferValue string  `json:"processing_transfer_value"`
		ProcessingRealPercent   int     `json:"processing_real_percent"`
		ProcessingBonusPercent  int     `json:"processing_bonus_percent"`
		BetMin                  *string `json:"bet_min"`
		BetMax                  *string `json:"bet_max"`
		Threshold               string  `json:"threshold"`
	} `json:"player_bonus"`
}

type PlayerBonusEvent struct {
	ID             int64  `db:"id"`
	Env            string `db:"env"`
	KafkaTopic     string `db:"kafka_topic"`
	KafkaPartition int    `db:"kafka_partition"`
	KafkaOffset    int64  `db:"kafka_offset"`

	ProcessedAt time.Time `db:"processed_at"`

	PlayerBonusID uuid.UUID      `db:"player_bonus_id"`
	PlayerID      uuid.UUID      `db:"player_id"`
	BonusID       uuid.UUID      `db:"bonus_id"`
	BonusCategory sql.NullString `db:"bonus_category"`
	Currency      sql.NullString `db:"currency"`
	NodeID        uuid.UUID      `db:"node_id"`

	Balance                 decimal.NullDecimal `db:"balance"`
	Wager                   decimal.NullDecimal `db:"wager"`
	ProcessingTransferType  sql.NullInt32       `db:"processing_transfer_type"`
	ProcessingTransferValue decimal.NullDecimal `db:"processing_transfer_value"`
	ProcessingRealPercent   sql.NullInt32       `db:"processing_real_percent"`
	ProcessingBonusPercent  sql.NullInt32       `db:"processing_bonus_percent"`
	BetMin                  decimal.NullDecimal `db:"bet_min"`
	BetMax                  decimal.NullDecimal `db:"bet_max"`
	Threshold               decimal.NullDecimal `db:"threshold"`

	EventType string `db:"event_type"`

	SchemaOK    bool           `db:"schema_ok"`
	SchemaError sql.NullString `db:"schema_error"`

	Label        sql.NullString `db:"label"`
	LabelComment sql.NullString `db:"label_comment"`
	LabelUser    sql.NullString `db:"label_user"`
	LabelTS      sql.NullTime   `db:"label_ts"`

	RawMessage json.RawMessage `db:"raw_message"`
}

func ParsePlayerBonusMessage(data []byte) (*PlayerBonusMessage, error) {
	var msg PlayerBonusMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
	return &msg, nil
}

func (pbm *PlayerBonusMessage) ToPlayerBonusEvent(env, topic string, partition int, offset int64, rawJSON []byte) (*PlayerBonusEvent, error) {
	ev := &PlayerBonusEvent{
		Env:            env,
		KafkaTopic:     topic,
		KafkaPartition: partition,
		KafkaOffset:    offset,
		ProcessedAt:    time.Now().UTC(),
		SchemaOK:       true,
		RawMessage:     rawJSON,
	}

	playerBonusID, err := uuid.Parse(pbm.PlayerBonus.ID)
	if err != nil {
		return nil, fmt.Errorf("invalid player_bonus_id: %w", err)
	}
	ev.PlayerBonusID = playerBonusID

	playerID, err := uuid.Parse(pbm.PlayerBonus.PlayerID)
	if err != nil {
		return nil, fmt.Errorf("invalid player_id: %w", err)
	}
	ev.PlayerID = playerID

	bonusID, err := uuid.Parse(pbm.PlayerBonus.BonusID)
	if err != nil {
		return nil, fmt.Errorf("invalid bonus_id: %w", err)
	}
	ev.BonusID = bonusID

	nodeID, err := uuid.Parse(pbm.PlayerBonus.Node)
	if err != nil {
		return nil, fmt.Errorf("invalid node_id: %w", err)
	}
	ev.NodeID = nodeID

	ev.EventType = pbm.Message.EventType

	if pbm.PlayerBonus.BonusCategory != "" {
		ev.BonusCategory = sql.NullString{String: pbm.PlayerBonus.BonusCategory, Valid: true}
	}
	if pbm.PlayerBonus.Currency != "" {
		ev.Currency = sql.NullString{String: pbm.PlayerBonus.Currency, Valid: true}
	}

	if pbm.PlayerBonus.Balance != "" {
		val, err := decimal.NewFromString(pbm.PlayerBonus.Balance)
		if err != nil {
			return nil, fmt.Errorf("invalid balance: %w", err)
		}
		ev.Balance = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	if pbm.PlayerBonus.Wager != "" {
		val, err := decimal.NewFromString(pbm.PlayerBonus.Wager)
		if err != nil {
			return nil, fmt.Errorf("invalid wager: %w", err)
		}
		ev.Wager = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	ev.ProcessingTransferType = sql.NullInt32{Int32: int32(pbm.PlayerBonus.ProcessingTransferType), Valid: true}

	if pbm.PlayerBonus.ProcessingTransferValue != "" {
		val, err := decimal.NewFromString(pbm.PlayerBonus.ProcessingTransferValue)
		if err != nil {
			return nil, fmt.Errorf("invalid processing_transfer_value: %w", err)
		}
		ev.ProcessingTransferValue = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	ev.ProcessingRealPercent = sql.NullInt32{Int32: int32(pbm.PlayerBonus.ProcessingRealPercent), Valid: true}
	ev.ProcessingBonusPercent = sql.NullInt32{Int32: int32(pbm.PlayerBonus.ProcessingBonusPercent), Valid: true}

	if pbm.PlayerBonus.BetMin != nil && *pbm.PlayerBonus.BetMin != "" {
		val, err := decimal.NewFromString(*pbm.PlayerBonus.BetMin)
		if err != nil {
			return nil, fmt.Errorf("invalid bet_min: %w", err)
		}
		ev.BetMin = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	if pbm.PlayerBonus.BetMax != nil && *pbm.PlayerBonus.BetMax != "" {
		val, err := decimal.NewFromString(*pbm.PlayerBonus.BetMax)
		if err != nil {
			return nil, fmt.Errorf("invalid bet_max: %w", err)
		}
		ev.BetMax = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	if pbm.PlayerBonus.Threshold != "" {
		val, err := decimal.NewFromString(pbm.PlayerBonus.Threshold)
		if err != nil {
			return nil, fmt.Errorf("invalid threshold: %w", err)
		}
		ev.Threshold = decimal.NullDecimal{Decimal: val, Valid: true}
	}

	return ev, nil
}
