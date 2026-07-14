# Curl E2E tests

Приёмочные E2E-скрипты проверяют полный путь телеметрии как black box:

```text
HTTP -> Ingestion -> Kafka -> Processing -> Redis / PostgreSQL -> Processing REST API
```

PowerShell-версия `curl-e2e.ps1` сохранена. Основной переносимый сценарий для
Git Bash и Linux находится в `curl-e2e.sh`.

## Требования

- Git Bash на Windows или Bash на Linux;
- `curl`;
- `jq`;
- `date` и `mktemp` (входят в Git Bash/coreutils);
- Docker Desktop с Docker Compose v2 — для `--compose` и DLQ-проверки.

DLQ-проверка запускается, когда доступен Docker Compose и контейнер `kafka`
текущего проекта работает. Иначе она выводится как `Skipped`; это не считается
успешно выполненной проверкой и явно отражается в итоговой статистике.

## Запуск из Git Bash

Из корня репозитория:

```bash
./tests/e2e/curl-e2e.sh
```

С автоматической сборкой и запуском системы:

```bash
./tests/e2e/curl-e2e.sh --compose
```

С пользовательскими URL:

```bash
./tests/e2e/curl-e2e.sh \
  --ingestion-url http://localhost:8080 \
  --processing-url http://localhost:8081
```

Таймаут polling можно задать аргументом:

```bash
./tests/e2e/curl-e2e.sh --timeout 120
```

Скрипт можно запускать и из `tests/e2e`; корень репозитория определяется по
расположению файла, а не по текущему каталогу.

Поддерживаются переменные окружения:

```text
INGESTION_BASE_URL
PROCESSING_BASE_URL
E2E_TIMEOUT_SECONDS
```

Приоритет значений: аргумент CLI, переменная окружения, значение по умолчанию.
По умолчанию используются `http://localhost:8080`, `http://localhost:8081` и
таймаут 90 секунд.

Для установки executable bit в Git:

```bash
git update-index --chmod=+x tests/e2e/curl-e2e.sh
```

## Что проверяется

- health Ingestion и Processing;
- Prometheus metrics Processing;
- готовность Kafka publishing path;
- границы публичных маршрутов сервисов;
- все ошибки JSON/schema validation из PowerShell-сценария;
- batch из 10 измерений;
- idempotency до rate limiting;
- rate limiting и `Retry-After`;
- сохранение telemetry в PostgreSQL через Processing REST API без дублей;
- создание `HIGH_HEART_RATE`, точный `triggered_at` и глобальный список alerts;
- alert deduplication при сохранении новой telemetry;
- DLQ diagnostic envelope и продвижение consumer после poison message.

Каждый запуск использует уникальный `RUN_ID` во всех `device_id`, `patient_id`
и `batch_id`, поэтому повторные запуски не конфликтуют с Redis и данными
предыдущих запусков.

## PowerShell-версия

До подтверждения эквивалентности Bash-версии прежний сценарий можно запускать:

```powershell
powershell -ExecutionPolicy Bypass -File .\tests\e2e\curl-e2e.ps1
```

или с запуском Compose:

```powershell
powershell -ExecutionPolicy Bypass -File .\tests\e2e\curl-e2e.ps1 -Compose
```
