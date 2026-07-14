# CONTRACT.md

Данный документ является единым источником истины для разработчиков
проекта.

Он описывает: - архитектуру взаимодействия сервисов; - публичные
контракты; - зоны ответственности; - правила разработки.

Любое изменение HTTP API, Kafka Contract, PostgreSQL Schema или Redis
Key Schema считается **breaking change**.

------------------------------------------------------------------------

# Общая архитектура

``` text
Generator -> HTTP -> Ingestion -> Kafka -> Processing
                                 |             |
                               Redis      Redis + PostgreSQL
```

## Data Flow

``` text
Generator
  ↓
POST /api/v1/telemetry
  ↓
Ingestion
 ├─ Validate Batch
 ├─ Redis (Idempotency)
 ├─ Redis (Rate Limit)
 └─ Kafka Producer
          ↓
   telemetry.raw
          ↓
Processing
 ├─ Redis (Sliding Window)
 ├─ Detector
 ├─ Redis (Alert Deduplication)
 ├─ PostgreSQL
 └─ Commit Kafka Offset
```

## Основные принципы

-   Ingestion и Processing не взаимодействуют напрямую.
-   Единственный канал взаимодействия --- Kafka.
-   PostgreSQL принадлежит только Processing.
-   Redis используется каждым сервисом независимо.
-   Redis --- временное in-memory хранилище.
-   PostgreSQL --- долговременное хранилище.
-   Сообщения передаются в JSON.

------------------------------------------------------------------------

# Ответственность сервисов

## Ingestion

Отвечает за: - HTTP API; - валидацию batch; - idempotency; - rate
limiting; - публикацию сообщений в Kafka.

Не отвечает за: - обработку телеметрии; - PostgreSQL; - Alert.

## Processing

Отвечает за: - Kafka Consumer Group; - обработку телеметрии; - анализ
состояния пациента; - генерацию Alert; - запись в PostgreSQL.

Особенности: - stateless; - Sliding Window хранится в Redis.

------------------------------------------------------------------------

# HTTP Contract

POST /api/v1/telemetry

Batch: - размер: 1..10 измерений; - timestamp: RFC3339 UTC; -
heart_rate: 20..250.

Ответы: - 202 Accepted - 400 Bad Request - 429 Too Many Requests - 503
Service Unavailable

------------------------------------------------------------------------

# Kafka Contract

Topics:

-   telemetry.raw
-   telemetry.dlq

Partition Key:

patient_id

Каждое измерение внутри HTTP batch публикуется в Kafka как отдельное
сообщение.

Message:

``` json
{
  "event_id":"...",
  "device_id":"...",
  "patient_id":"...",
  "timestamp":"...",
  "heart_rate":78
}
```

DLQ Message (`telemetry.dlq`):

``` json
{
  "reason":"...",
  "timestamp":"...",
  "source_topic":"telemetry.raw",
  "source_partition":0,
  "source_offset":0,
  "payload_base64":"..."
}
```

------------------------------------------------------------------------

# Redis

## Ingestion

Idempotency

    idempotency:{device_id}:{batch_id}

TTL: 24h

Используется для защиты от повторной публикации batch.

Rate Limiting

    rate:{device_id}

Redis token bucket:

    refill: 1 token / 5 секунд
    capacity: 2 batch

После восстановления соединения допускается burst до 2 batch.

## Processing

Sliding Window

    processing:window:{patient_id}
    processing:window:{patient_id}:events

TTL: 5 минут.

Хранит последние 60 секунд измерений.

`processing:window:{patient_id}` is a Redis Sorted Set:
score = unix timestamp, member = event_id.

`processing:window:{patient_id}:events` is a Redis Hash:
field = event_id, value = serialized telemetry event.

Detector анализирует окно и принимает решение о создании Alert.

Alert Deduplication

    processing:alert:dedup:{patient_id}:HIGH_HEART_RATE

TTL: 5 минут.

Не допускает генерацию одинаковых Alert подряд.

Ключи Redis каждого сервиса используются только самим сервисом.

------------------------------------------------------------------------

# PostgreSQL

Используется только Processing.

Хранит:

-   telemetry
-   alerts

Retention: 30 дней.

------------------------------------------------------------------------

# Generator

-   отдельная утилита `cmd/generator` и Compose profile `load`;
-   endpoint внутри Compose: `POST http://ingestion:8080/api/v1/telemetry`;
-   режимы: `normal`, `high-heart-rate`, `mixed`, `duplicate`,
    `ramp-up`, `spike`, `soak`, `chaos-ready`;
-   каждое активное устройство создаёт измерение каждые 5 секунд;
-   обычный batch = 1 измерение, `batch_id` уникален между запусками;
-   `patient_id` стабилен для устройства;
-   рост RPS достигается числом устройств, а не уменьшением интервала
    одного устройства;
-   в режиме `duplicate` тот же `device_id + batch_id` отправляется повторно;
-   при `429` учитывается `Retry-After`, при `503` и сетевой ошибке
    используется retry с exponential backoff;
-   неотправленные измерения остаются в локальном in-memory буфере;
-   после восстановления связи буфер отправляется batch-ами до 10 измерений;
-   результат запуска сохраняется в JSON, принятые `batch_id` и `event_id` —
    в соседний JSONL-файл.

Основные CLI/ENV параметры:

-   `--target-url` / `GENERATOR_TARGET_URL`;
-   `--mode` / `GENERATOR_MODE`;
-   `--devices` / `GENERATOR_DEVICES`;
-   `--patients` / `GENERATOR_PATIENTS`;
-   `--duration` / `GENERATOR_DURATION`;
-   `--batch-size` / `GENERATOR_BATCH_SIZE`;
-   `--max-batch-size` / `GENERATOR_MAX_BATCH_SIZE`;
-   `--base-interval` / `GENERATOR_BASE_INTERVAL`;
-   `--rps-limit` / `GENERATOR_RPS_LIMIT`;
-   `--output` / `GENERATOR_OUTPUT`.

------------------------------------------------------------------------

# Обработка ошибок

Ingestion: - 400 Validation Error - 429 Rate Limit или Publisher
Backpressure с Retry-After - 503 Kafka unavailable

Processing: - битые сообщения -\> telemetry.dlq - ошибка PostgreSQL -\>
Offset не подтверждается.

------------------------------------------------------------------------

# Подтверждение обработки

Offset подтверждается только после успешной обработки сообщения и записи
в PostgreSQL.

Семантика: at-least-once.

Мониторинг:

-   Kafka Consumer Lag
-   Prometheus (`/metrics` для Ingestion и Processing)

Grafana и dashboards не входят в MVP.

------------------------------------------------------------------------

# Правила разработки

Запрещено: - прямые вызовы между сервисами; - общие таблицы
PostgreSQL; - общие Redis Keys; - обратные Kafka ACK.

Разрешено: - internal/shared; - общий Kafka Contract; - Docker Compose.

------------------------------------------------------------------------

# Версионирование

Изменение HTTP API, Kafka Topics, Kafka Message Schema, PostgreSQL
Schema или Redis Key Schema считается breaking change.
