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
