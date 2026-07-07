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

------------------------------------------------------------------------

# Redis

## Ingestion

Idempotency

    idempotency:{device_id}:{batch_id}

TTL: 24h

Используется для защиты от повторной публикации batch.

Rate Limiting

    rate:{device_id}

Лимит:

    1 batch / 5 секунд

Допускается burst после восстановления соединения.

## Processing

Sliding Window

    window:{patient_id}:heart_rate

TTL: 2 минуты.

Хранит последние 60 секунд измерений.

Detector анализирует окно и принимает решение о создании Alert.

Alert Deduplication

    alert:{patient_id}:HIGH_HEART_RATE

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

-   измерение каждые 5 секунд;
-   отправка каждые 5 секунд;
-   обычный batch = 1 измерение;
-   offline -\> локальный буфер;
-   после восстановления сети отправка batch до 10 измерений.

------------------------------------------------------------------------

# Обработка ошибок

Ingestion: - 400 Validation Error - 429 Rate Limit - 503 Kafka
unavailable

Processing: - битые сообщения -\> telemetry.dlq - ошибка PostgreSQL -\>
Offset не подтверждается.

------------------------------------------------------------------------

# Подтверждение обработки

Offset подтверждается только после успешной обработки сообщения и записи
в PostgreSQL.

Семантика: at-least-once.

Мониторинг:

-   Kafka Consumer Lag
-   Prometheus
-   Grafana

------------------------------------------------------------------------

# Правила разработки

Запрещено: - прямые вызовы между сервисами; - общие таблицы
PostgreSQL; - общие Redis Keys; - обратные Kafka ACK.

Разрешено: - internal/shared; - общий Kafka Contract; - Docker Compose.

------------------------------------------------------------------------

# Версионирование

Изменение HTTP API, Kafka Topics, Kafka Message Schema, PostgreSQL
Schema или Redis Key Schema считается breaking change.
