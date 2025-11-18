CREATE TABLE bonus_events_validation (
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

    CONSTRAINT uniq_env_topic_partition_offset
        UNIQUE (env, kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX idx_env_seqkey_event_ts
    ON bonus_events_validation (env, seq_key, event_ts);

CREATE TABLE bonus_sequence_state (
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

    last_validation_id      BIGINT REFERENCES bonus_events_validation(id),

    PRIMARY KEY (env, seq_key)
);

CREATE INDEX idx_seq_state_last_ts
    ON bonus_sequence_state (env, last_event_ts);
