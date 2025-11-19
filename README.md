# AI-Validator

Real-time Kafka consumer для валидации и сбора датасета событий бонусной системы под обучение ML-моделей.

## Концепция

**Цель**: Собрать качественный датасет событий из Kafka с валидацией схемы, последовательности и инвариантов для обучения моделей аномальной детекции.

**Архитектура**:
- **Kafka Consumer** → читает события из топиков `bonus.v2.info.Bonus` и `bonus.v1.playerBonus`
- **Validator/Processor** → валидирует схему JSON, проверяет последовательность событий (timestamp, offset)
- **PostgreSQL** → сохраняет события с метками валидации + snapshot последнего состояния для каждого `seq_key` (bonus.id)

**Основные возможности**:
1. Валидация JSON-схемы событий
2. Проверка последовательности событий (event_ts не идёт назад, offset не уменьшается)
3. Хранение предыдущего состояния (prev_balance, prev_wager, etc.) для feature engineering
4. Батчинг обработки с ручным коммитом офсетов
5. Обработка ошибок по партициям (при infra error в партиции — пропускаем остальные сообщения партиции)
6. Снятие датасета для обучения ML

---

## Схема БД

### bonus_info_bonus_events

Таблица событий для топика `bonus.v2.info.Bonus`.

```sql
CREATE TABLE bonus_info_bonus_events (
    id                   BIGSERIAL PRIMARY KEY,

    -- Kafka metadata
    env                  TEXT        NOT NULL,
    kafka_topic          TEXT        NOT NULL,
    kafka_partition      INTEGER     NOT NULL,
    kafka_offset         BIGINT      NOT NULL,
    processed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_ts             TIMESTAMPTZ,

    -- Business keys
    seq_key              UUID        NOT NULL,  -- bonus.id (ключ для построения sequence)
    player_id            UUID        NOT NULL,
    node_id              UUID        NOT NULL,
    bonus_id             UUID        NOT NULL,

    -- Event data
    event_type           TEXT        NOT NULL,  -- playerBonusCreate, playerBonusUpdate
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

    -- Previous state (для feature engineering и проверки последовательности)
    prev_event_type          TEXT,
    prev_player_bonus_status INTEGER,
    prev_balance             NUMERIC(20,2),
    prev_wager               NUMERIC(20,2),
    prev_total_wager         NUMERIC(20,2),
    prev_event_ts            TIMESTAMPTZ,
    prev_kafka_offset        BIGINT,

    -- Validation flags
    schema_ok            BOOLEAN     NOT NULL DEFAULT TRUE,
    schema_error         TEXT,
    sequence_ok          BOOLEAN     NOT NULL DEFAULT TRUE,
    sequence_reason      TEXT,

    -- ML model fields (заполняются позже ML-pipeline)
    model_version        TEXT,
    anomaly_score        DOUBLE PRECISION,
    is_anomaly           BOOLEAN,
    anomaly_reason       TEXT,

    -- Manual labeling (для обучения с учителем)
    label                TEXT,
    label_comment        TEXT,
    label_user           TEXT,
    label_ts             TIMESTAMPTZ,

    raw_message          JSONB,

    CONSTRAINT uniq_bonus_info_bonus_env_topic_partition_offset
        UNIQUE (env, kafka_topic, kafka_partition, kafka_offset)
);
```

**Индексы**:
- `idx_bonus_info_bonus_env_seqkey_event_ts` — для выборки истории по seq_key
- `idx_bonus_info_bonus_label_processed` — для экспорта размеченных данных
- `idx_bonus_info_bonus_validation_status` — для выборки только валидных/невалидных событий

---

### bonus_info_bonus_sequence

Snapshot-таблица для хранения последнего состояния каждого `seq_key` (bonus.id). Используется для проверки последовательности и заполнения `prev_*` полей.

```sql
CREATE TABLE bonus_info_bonus_sequence (
    env                     TEXT    NOT NULL,
    seq_key                 UUID    NOT NULL,  -- bonus.id

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
```

---

### bonus_player_bonus_events

Таблица событий для топика `bonus.v1.playerBonus` (более старый формат, без проверки последовательности).

```sql
CREATE TABLE bonus_player_bonus_events (
    id                      BIGSERIAL PRIMARY KEY,

    env                     TEXT        NOT NULL,
    kafka_topic             TEXT        NOT NULL,
    kafka_partition         INTEGER     NOT NULL,
    kafka_offset            BIGINT      NOT NULL,
    processed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    player_bonus_id         UUID        NOT NULL,
    player_id               UUID        NOT NULL,
    bonus_id                UUID        NOT NULL,
    bonus_category          TEXT,
    currency                VARCHAR(10),
    node_id                 UUID        NOT NULL,
    balance                 NUMERIC(20,2),
    wager                   NUMERIC(20,2),
    processing_transfer_type    INTEGER,
    processing_transfer_value   NUMERIC(20,2),
    processing_real_percent     INTEGER,
    processing_bonus_percent    INTEGER,
    bet_min                 NUMERIC(20,2),
    bet_max                 NUMERIC(20,2),
    threshold               NUMERIC(20,2),
    event_type              TEXT        NOT NULL,

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
```

---

## Запуск через Docker Compose

### Предварительные требования

- Docker 20+
- Docker Compose 1.27+

### Шаги запуска

1. **Клонируйте репозиторий**:
   ```bash
   git clone <repo-url>
   cd AI-validator
   ```

2. **Настройте конфигурацию** `config.yaml`:
   ```yaml
   env: "beta-10"
   log_level: "info"

   kafka:
     brokers:
       - "kafka-broker-1:9092"
       - "kafka-broker-2:9092"
     bonus_info_bonus:
       topic: "beta-10_bonus.v2.info.Bonus"
       group_id: "ai-validator"
       batch_size: 100
       batch_max_wait_ms: 1000
     bonus_player_bonus:
       topic: "beta-10_bonus.v1.playerBonus"
       group_id: "ai-validator"
       batch_size: 100
       batch_max_wait_ms: 1000

   db:
     dsn: "postgres://user:pass@postgres:5432/ai_validator?sslmode=disable"
   ```

3. **Запустите сервисы**:
   ```bash
   docker-compose up -d
   ```

4. **Проверьте логи**:
   ```bash
   docker-compose logs -f ai-validator
   ```

5. **Проверьте health-check**:
   ```bash
   curl http://localhost:8080/health
   ```

   Ответ:
   ```json
   {"status":"healthy","database":"ok","kafka":"connected"}
   ```

---

## Примеры конфигурации

### Локальная разработка

```yaml
env: "local"
log_level: "debug"

kafka:
  brokers:
    - "localhost:9092"
  bonus_info_bonus:
    topic: "bonus.v2.info.Bonus"
    group_id: "ai-validator-local"
    batch_size: 50
    batch_max_wait_ms: 500
  bonus_player_bonus:
    topic: "bonus.v1.playerBonus"
    group_id: "ai-validator-local"
    batch_size: 50
    batch_max_wait_ms: 500

db:
  dsn: "postgres://user:pass@localhost:5432/ai_validator?sslmode=disable"
```

### Production

```yaml
env: "production"
log_level: "warn"

kafka:
  brokers:
    - "kafka-prod-1:9092"
    - "kafka-prod-2:9092"
    - "kafka-prod-3:9092"
  bonus_info_bonus:
    topic: "prod_bonus.v2.info.Bonus"
    group_id: "ai-validator-prod"
    batch_size: 500
    batch_max_wait_ms: 2000
  bonus_player_bonus:
    topic: "prod_bonus.v1.playerBonus"
    group_id: "ai-validator-prod"
    batch_size: 500
    batch_max_wait_ms: 2000

db:
  dsn: "postgres://user:pass@postgres-prod:5432/ai_validator?sslmode=require"
```

---

