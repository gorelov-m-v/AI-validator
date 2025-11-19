-- bonus.v2.info.Bonus topic: events log table
CREATE TABLE bonus_info_bonus_events (
    id                   BIGSERIAL PRIMARY KEY,

    env                  TEXT        NOT NULL,
    kafka_topic          TEXT        NOT NULL,
    kafka_partition      INTEGER     NOT NULL,
    kafka_offset         BIGINT      NOT NULL,

    processed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_ts             TIMESTAMPTZ,

    seq_key              UUID        NOT NULL,  -- bonus.id
    player_id            UUID        NOT NULL,  -- bonus.playerId
    node_id              UUID        NOT NULL,  -- bonus.nodeId
    bonus_id             UUID        NOT NULL,  -- bonus.bonusId

    event_type           TEXT        NOT NULL,  -- message.eventType
    bonus_category       TEXT,
    currency             VARCHAR(10),

    received_balance     NUMERIC(20,2),
    balance              NUMERIC(20,2),
    wager                NUMERIC(20,2),
    total_wager          NUMERIC(20,2),

    player_bonus_status  INTEGER,
    expired_at           BIGINT,
    created_at           BIGINT,
    updated_at           BIGINT,
    is_wager             BOOLEAN,

    prev_event_type          TEXT,
    prev_player_bonus_status INTEGER,
    prev_balance             NUMERIC(20,2),
    prev_wager               NUMERIC(20,2),
    prev_total_wager         NUMERIC(20,2),
    prev_event_ts            TIMESTAMPTZ,
    prev_kafka_offset        BIGINT,

    schema_ok            BOOLEAN     NOT NULL DEFAULT TRUE,
    schema_error         TEXT,

    model_version        TEXT,
    anomaly_score        DOUBLE PRECISION,
    is_anomaly           BOOLEAN,
    anomaly_reason       TEXT,

    sequence_ok          BOOLEAN     NOT NULL DEFAULT TRUE,
    sequence_reason      TEXT,

    label                TEXT,
    label_comment        TEXT,
    label_user           TEXT,
    label_ts             TIMESTAMPTZ,

    raw_message          JSONB,

    CONSTRAINT uniq_bonus_info_bonus_env_topic_partition_offset
        UNIQUE (env, kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX idx_bonus_info_bonus_env_seqkey_event_ts
    ON bonus_info_bonus_events (env, seq_key, event_ts);

CREATE INDEX idx_bonus_info_bonus_label_processed
    ON bonus_info_bonus_events (env, label, processed_at);

CREATE INDEX idx_bonus_info_bonus_validation_status
    ON bonus_info_bonus_events (schema_ok, sequence_ok, env, processed_at);

-- bonus.v2.info.Bonus topic: sequence state snapshot table
CREATE TABLE bonus_info_bonus_sequence (
    env                     TEXT    NOT NULL,
    seq_key                 UUID    NOT NULL,

    last_event_ts           TIMESTAMPTZ NOT NULL,
    last_kafka_topic        TEXT        NOT NULL,
    last_kafka_partition    INTEGER     NOT NULL,
    last_kafka_offset       BIGINT      NOT NULL,

    last_event_type         TEXT,
    last_player_bonus_status INTEGER,
    last_balance            NUMERIC(20,2),
    last_wager              NUMERIC(20,2),
    last_total_wager        NUMERIC(20,2),

    last_validation_id      BIGINT REFERENCES bonus_info_bonus_events(id),

    PRIMARY KEY (env, seq_key)
);

CREATE INDEX idx_bonus_info_bonus_seq_last_ts
    ON bonus_info_bonus_sequence (env, last_event_ts);

-- bonus.v1.playerBonus topic: events log table
CREATE TABLE bonus_player_bonus_events (
    id                      BIGSERIAL PRIMARY KEY,

    env                     TEXT        NOT NULL,
    kafka_topic             TEXT        NOT NULL,
    kafka_partition         INTEGER     NOT NULL,
    kafka_offset            BIGINT      NOT NULL,

    processed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    player_bonus_id         UUID        NOT NULL,  -- player_bonus.id
    player_id               UUID        NOT NULL,  -- player_bonus.player_id
    bonus_id                UUID        NOT NULL,  -- player_bonus.bonus_id
    bonus_category          TEXT,                  -- player_bonus.bonus_category
    currency                VARCHAR(10),           -- player_bonus.currency
    node_id                 UUID        NOT NULL,  -- player_bonus.node

    balance                 NUMERIC(20,2),         -- player_bonus.balance
    wager                   NUMERIC(20,2),         -- player_bonus.wager
    processing_transfer_type    INTEGER,           -- player_bonus.processing_transfer_type
    processing_transfer_value   NUMERIC(20,2),     -- player_bonus.processing_transfer_value
    processing_real_percent     INTEGER,           -- player_bonus.processing_real_percent
    processing_bonus_percent    INTEGER,           -- player_bonus.processing_bonus_percent
    bet_min                 NUMERIC(20,2),         -- player_bonus.bet_min
    bet_max                 NUMERIC(20,2),         -- player_bonus.bet_max
    threshold               NUMERIC(20,2),         -- player_bonus.threshold

    event_type              TEXT        NOT NULL,  -- message.event_type

    schema_ok               BOOLEAN     NOT NULL DEFAULT TRUE,
    schema_error            TEXT,

    label                   TEXT,
    label_comment           TEXT,
    label_user              TEXT,
    label_ts                TIMESTAMPTZ,

    raw_message             JSONB,

    CONSTRAINT uniq_bonus_player_bonus_env_topic_partition_offset
        UNIQUE (env, kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX idx_bonus_player_bonus_env_player_bonus_id
    ON bonus_player_bonus_events (env, player_bonus_id);

CREATE INDEX idx_bonus_player_bonus_env_player_id
    ON bonus_player_bonus_events (env, player_id);

CREATE INDEX idx_bonus_player_bonus_env_processed
    ON bonus_player_bonus_events (env, processed_at);
