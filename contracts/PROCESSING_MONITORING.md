# Processing Monitoring Contract

Документ фиксирует, что сервис Processing отдаёт стажёру A для Prometheus,
Grafana и нагрузочных испытаний.

## Зоны ответственности

Стажёр Б отвечает за:

- endpoint `/metrics` в Processing;
- метрики Kafka consumer, DLQ, alerts, Redis, PostgreSQL и latency обработки;
- PromQL-запросы для Processing dashboard;
- проверку at-least-once, lag recovery, spike/soak/chaos на стороне Processing;
- раздел отчёта по Processing.

Стажёр A отвечает за:

- контейнеры Prometheus и Grafana в `docker-compose.yml`;
- `prometheus.yml` и scrape target для Processing;
- Grafana datasource и dashboard provisioning;
- общий generator нагрузки и сценарии HTTP-нагрузки через Ingestion.

## Endpoint

Processing обязан отдавать Prometheus text exposition:

```text
GET http://processing:8081/metrics
```

Для локального запуска с проброшенным портом:

```text
GET http://localhost:8081/metrics
```

Рекомендуемый scrape interval:

```text
5s
```

Prometheus scrape job для стажёра A:

```yaml
scrape_configs:
  - job_name: processing
    scrape_interval: 5s
    static_configs:
      - targets:
          - processing:8081
```

## Метрики

| Metric | Type | Labels | Что означает |
| --- | --- | --- | --- |
| `processing_kafka_messages_total` | counter | `status=processed,error,dlq` | Сколько Kafka-сообщений Processing успешно обработал, отправил в DLQ или получил с ошибкой обработки. |
| `processing_kafka_commits_total` | counter | `status=success,error` | Успешные и неуспешные manual commit offset. |
| `processing_dlq_messages_total` | counter | `reason=decode,validation,other` | Сколько сообщений отправлено в `telemetry.dlq`. |
| `processing_alerts_created_total` | counter | `type=HIGH_HEART_RATE` | Сколько alert создал Processing. |
| `processing_errors_total` | counter | `stage=fetch,decode,validate,postgres,redis,commit,dlq` | Ошибки по стадиям pipeline. |
| `processing_kafka_consumer_lag` | gauge | `topic`, `partition` | Приближённый lag по последнему прочитанному сообщению: `HighWaterMark - Offset - 1`. |
| `processing_processing_duration_seconds` | histogram | none | Время обработки одного валидного Kafka-сообщения внутри Processing usecase. |
| `processing_postgres_write_duration_seconds` | histogram | `operation=save_telemetry,save_alert` | Latency записи в PostgreSQL. |
| `processing_redis_duration_seconds` | histogram | `operation=sliding_window_add,alert_dedup_reserve` | Latency операций Redis. |
| `processing_sliding_window_events_current` | gauge | none | Размер последнего обработанного sliding window. |

Важно: `processing_kafka_consumer_lag` является практической per-message
оценкой lag по high watermark из `kafka-go`. Для защиты достаточно показать:

- при spike lag растёт;
- после завершения spike lag уменьшается;
- после restart Processing lag восстанавливается к норме.

## PromQL Для Grafana

Throughput Processing:

```promql
sum(rate(processing_kafka_messages_total{status="processed"}[1m]))
```

p95 latency обработки:

```promql
histogram_quantile(
  0.95,
  sum(rate(processing_processing_duration_seconds_bucket[5m])) by (le)
)
```

Kafka consumer lag:

```promql
sum(processing_kafka_consumer_lag)
```

DLQ rate:

```promql
sum(rate(processing_dlq_messages_total[1m]))
```

Alerts rate:

```promql
sum(rate(processing_alerts_created_total[1m]))
```

Errors by stage:

```promql
sum(rate(processing_errors_total[1m])) by (stage)
```

PostgreSQL p95 write latency:

```promql
histogram_quantile(
  0.95,
  sum(rate(processing_postgres_write_duration_seconds_bucket[5m])) by (le, operation)
)
```

Redis p95 latency:

```promql
histogram_quantile(
  0.95,
  sum(rate(processing_redis_duration_seconds_bucket[5m])) by (le, operation)
)
```

Kafka commits:

```promql
sum(rate(processing_kafka_commits_total[1m])) by (status)
```

Processing service up/down:

```promql
up{job="processing"}
```

## Grafana Dashboard

Стажёр A должен собрать dashboard `Processing Service` из панелей:

1. Processing service up/down.
2. Processed messages/sec.
3. Kafka consumer lag.
4. p95 processing latency.
5. PostgreSQL p95 write latency by operation.
6. Redis p95 latency by operation.
7. DLQ messages/sec.
8. Errors by stage.
9. Alerts/sec.
10. Kafka commits success/error.

## Требования К Generator Для Проверки Processing

Стажёр A пишет generator, но для Processing нужны режимы:

| Mode | Зачем нужен Processing |
| --- | --- |
| `normal` | Проверить стабильную обработку без alerts. |
| `high-heart-rate` | Создать `HIGH_HEART_RATE`: heart rate > 120 минимум 60 секунд. |
| `mixed` | Одновременно нормальные пациенты, sustained high heart rate и короткие выбросы без alert. |
| `duplicate` | Проверить, что повторные batch не создают лишние alerts. |
| `ramp-up` | Найти предел Processing по msg/s, p95 и lag. |
| `spike` | Показать рост lag и восстановление после x5 всплеска. |
| `soak` | 30-60 минут на 70% предела без роста lag/errors. |
| `chaos-ready` | Нагрузка продолжается, пока Processing убивают и запускают обратно. |

Generator должен сохранять результаты запуска:

```json
{
  "mode": "spike",
  "devices": 1000,
  "patients": 1000,
  "duration_seconds": 600,
  "sent_batches": 123,
  "sent_measurements": 123,
  "accepted_202": 120,
  "rate_limited_429": 2,
  "unavailable_503": 1,
  "p95_http_latency_ms": 87,
  "throughput_measurements_per_sec": 4500
}
```

## At-Least-Once Proof

Сценарий для стажёра Б:

1. Запустить `normal` или `mixed` нагрузку.
2. Убедиться, что `processing_kafka_messages_total{status="processed"}` растёт.
3. Убить контейнер или процесс Processing.
4. Убедиться, что Ingestion продолжает принимать данные, а Kafka lag растёт.
5. Запустить Processing обратно.
6. Убедиться, что `processing_kafka_consumer_lag` уменьшается.
7. Сверить количество отправленных generator measurements с PostgreSQL `telemetry`.
8. Сверить, что alerts не размножаются при повторной обработке.
9. Сохранить графики lag, throughput, p95 latency и alerts.

Ожидаемый вывод:

```text
Processing подтверждает offset только после успешной обработки и записи в PostgreSQL.
При падении сервиса сообщения остаются в Kafka и обрабатываются после restart.
Семантика обработки: at-least-once.
```

## Load Testing Processing

### Ramp-up

Цель:

- найти предел Processing по `processed msg/s`;
- определить, где начинает стабильно расти lag или p95 latency.

Сохранить:

- max stable msg/s;
- p95 processing latency;
- max/avg consumer lag;
- errors/DLQ rate.

### Spike

Цель:

- проверить x5 всплеск;
- доказать, что lag растёт контролируемо и потом уменьшается.

Сохранить:

- график lag до spike, во время spike и после;
- время восстановления lag;
- p95 latency во время spike.

### Soak

Цель:

- 30-60 минут на 70% найденного предела;
- проверить, что lag, errors и latency не растут монотонно.

Сохранить:

- график throughput;
- график lag;
- график p95 latency;
- DLQ/errors rate.

### Chaos

Цель:

- убить Processing под нагрузкой;
- доказать restart/rebalance без потери данных.

Сохранить:

- момент остановки Processing;
- рост lag;
- момент restart;
- время восстановления lag;
- сверку PostgreSQL telemetry/alerts.

## Два Цикла Оптимизации

Каждый цикл оформить в отчёте по шаблону:

```text
Измерение:
...

Bottleneck:
...

Оптимизация:
...

Повторное измерение:
...

Результат:
...
```

Кандидаты для Processing:

- число Kafka partitions против числа Processing instances;
- PostgreSQL write latency;
- Redis sliding window latency;
- Kafka consumer fetch settings;
- pool соединений PostgreSQL/Redis;
- объём операций Redis на одно событие.

## Раздел Отчёта Стажёра Б

В отчёт по Processing включить:

- архитектуру Processing;
- consumer group и manual commit;
- at-least-once semantics;
- Redis sliding window;
- Redis alert deduplication;
- DLQ;
- PostgreSQL storage и retention;
- добавленные метрики;
- Grafana screenshots;
- ramp-up/spike/soak/chaos результаты;
- два цикла оптимизации;
- итоговые выводы по Processing.
