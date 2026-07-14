#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

DEFAULT_INGESTION_BASE_URL="http://localhost:8080"
DEFAULT_PROCESSING_BASE_URL="http://localhost:8081"
DEFAULT_TIMEOUT_SECONDS=90

INGESTION_BASE_URL="${INGESTION_BASE_URL:-$DEFAULT_INGESTION_BASE_URL}"
PROCESSING_BASE_URL="${PROCESSING_BASE_URL:-$DEFAULT_PROCESSING_BASE_URL}"
TIMEOUT_SECONDS="${E2E_TIMEOUT_SECONDS:-$DEFAULT_TIMEOUT_SECONDS}"
USE_COMPOSE=0

PASSED=0
FAILED=0
SKIPPED=0
SUMMARY_ENABLED=0
SUMMARY_PRINTED=0
TEMP_DIR=""

usage() {
  cat <<'EOF'
Usage: curl-e2e.sh [options]

Options:
  --ingestion-url URL   Ingestion base URL
  --processing-url URL  Processing base URL
  --timeout SECONDS     Overall polling timeout (positive integer)
  --compose             Run docker compose up -d --build before tests
  --help                Show this help

Configuration priority: CLI argument, environment variable, default.
Environment variables: INGESTION_BASE_URL, PROCESSING_BASE_URL,
E2E_TIMEOUT_SECONDS.
EOF
}

die() {
  printf '[FAIL] %s\n' "$*" >&2
  exit 1
}

print_summary() {
  if ((SUMMARY_PRINTED)); then
    return
  fi
  SUMMARY_PRINTED=1
  printf '\nE2E test summary\n'
  printf 'Passed: %d\n' "$PASSED"
  printf 'Failed: %d\n' "$FAILED"
  printf 'Skipped: %d\n' "$SKIPPED"
}

cleanup() {
  local exit_code=$?
  if [[ -n "$TEMP_DIR" && -d "$TEMP_DIR" ]]; then
    rm -rf -- "$TEMP_DIR"
  fi
  if ((SUMMARY_ENABLED)); then
    print_summary
  fi
  return "$exit_code"
}

trap cleanup EXIT

while (($# > 0)); do
  case "$1" in
    --ingestion-url)
      (($# >= 2)) || die "--ingestion-url requires a value"
      INGESTION_BASE_URL=$2
      shift 2
      ;;
    --processing-url)
      (($# >= 2)) || die "--processing-url requires a value"
      PROCESSING_BASE_URL=$2
      shift 2
      ;;
    --timeout)
      (($# >= 2)) || die "--timeout requires a value"
      TIMEOUT_SECONDS=$2
      shift 2
      ;;
    --compose)
      USE_COMPOSE=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1 (use --help)"
      ;;
  esac
done

[[ -n "$INGESTION_BASE_URL" ]] || die "ingestion URL must not be empty"
[[ -n "$PROCESSING_BASE_URL" ]] || die "processing URL must not be empty"
[[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "timeout must be a positive integer"

for required_command in bash curl jq date mktemp; do
  command -v "$required_command" >/dev/null 2>&1 || \
    die "required command '$required_command' was not found in PATH"
done

if ((USE_COMPOSE)); then
  command -v docker >/dev/null 2>&1 || die "required command 'docker' was not found in PATH"
  docker compose version >/dev/null 2>&1 || die "Docker Compose v2 ('docker compose') is not available"
fi

TEMP_DIR="$(mktemp -d)" || die "mktemp could not create a temporary directory"
SUMMARY_ENABLED=1

RUN_ID="$(date -u +%Y%m%d%H%M%S)-$RANDOM"
BASE_EPOCH="$(date -u +%s)"

if ! date -u -d "@$BASE_EPOCH" +"%Y-%m-%dT%H:%M:%SZ" >/dev/null 2>&1; then
  die "date does not support the Git Bash/GNU '-d @<unix_timestamp>' form"
fi

if ((TIMEOUT_SECONDS < 10)); then
  REQUEST_TIMEOUT=$TIMEOUT_SECONDS
else
  REQUEST_TIMEOUT=10
fi

join_url() {
  local base=${1%/}
  local path=${2#/}
  printf '%s/%s' "$base" "$path"
}

rfc3339_at() {
  local offset=$1
  date -u -d "@$((BASE_EPOCH + offset))" +"%Y-%m-%dT%H:%M:%SZ"
}

INGESTION_HEALTH_URL="$(join_url "$INGESTION_BASE_URL" /health)"
PROCESSING_HEALTH_URL="$(join_url "$PROCESSING_BASE_URL" /health)"
PROCESSING_METRICS_URL="$(join_url "$PROCESSING_BASE_URL" /metrics)"
INGESTION_TELEMETRY_URL="$(join_url "$INGESTION_BASE_URL" /api/v1/telemetry)"
PROCESSING_TELEMETRY_URL="$(join_url "$PROCESSING_BASE_URL" /telemetry)"
ALERTS_URL="$(join_url "$PROCESSING_BASE_URL" /alerts)"

HTTP_STATUS=""
HTTP_BODY=""
HTTP_HEADERS=""

http_request() {
  local method=$1
  local url=$2
  local body_file headers_file request_body_file status curl_exit

  body_file="$(mktemp "$TEMP_DIR/response-body.XXXXXX")"
  headers_file="$(mktemp "$TEMP_DIR/response-headers.XXXXXX")"
  request_body_file=""

  local -a curl_args=(
    --silent
    --show-error
    --request "$method"
    --dump-header "$headers_file"
    --output "$body_file"
    --write-out "%{http_code}"
    --connect-timeout 3
    --max-time "$REQUEST_TIMEOUT"
  )

  if (($# >= 3)); then
    request_body_file="$(mktemp "$TEMP_DIR/request-body.XXXXXX")"
    printf '%s' "$3" >"$request_body_file"
    curl_args+=(
      --header "Content-Type: application/json"
      --data-binary "@$request_body_file"
    )
  fi
  curl_args+=("$url")

  curl_exit=0
  status="$(curl "${curl_args[@]}")" || curl_exit=$?
  HTTP_BODY="$(<"$body_file")"
  HTTP_HEADERS="$(<"$headers_file")"

  if ((curl_exit != 0)); then
    printf 'curl failed with exit code %d for %s %s\n' "$curl_exit" "$method" "$url" >&2
    return 1
  fi
  if [[ ! "$status" =~ ^[0-9]{3}$ ]]; then
    printf "curl returned invalid HTTP status '%s' for %s %s\n" "$status" "$method" "$url" >&2
    return 1
  fi

  HTTP_STATUS=$status
}

get_response_header() {
  local wanted=${1,,}
  local line name value
  while IFS= read -r line; do
    line=${line%$'\r'}
    [[ "$line" == *:* ]] || continue
    name=${line%%:*}
    value=${line#*:}
    name=${name,,}
    value=${value#"${value%%[![:space:]]*}"}
    if [[ "$name" == "$wanted" ]]; then
      printf '%s' "$value"
      return 0
    fi
  done <<<"$HTTP_HEADERS"
  return 1
}

fail_assertion() {
  printf '%s\n' "$*" >&2
  return 1
}

assert_status() {
  local expected=$1
  [[ "$HTTP_STATUS" == "$expected" ]] || \
    fail_assertion "Expected HTTP $expected, got HTTP $HTTP_STATUS"$'\n'"Response body: $HTTP_BODY"
}

assert_status_in() {
  local expected
  for expected in "$@"; do
    [[ "$HTTP_STATUS" == "$expected" ]] && return 0
  done
  fail_assertion "Expected HTTP status in [$*], got HTTP $HTTP_STATUS"$'\n'"Response body: $HTTP_BODY"
}

assert_not_success() {
  if ((10#$HTTP_STATUS >= 200 && 10#$HTTP_STATUS < 300)); then
    fail_assertion "Expected a non-success status, got HTTP $HTTP_STATUS"$'\n'"Response body: $HTTP_BODY"
  fi
}

assert_json_content_type() {
  local content_type
  content_type="$(get_response_header content-type)" || content_type=""
  [[ "${content_type,,}" == *application/json* ]] || \
    fail_assertion "Expected JSON Content-Type, got '$content_type'"
  jq -e . >/dev/null 2>&1 <<<"$HTTP_BODY" || \
    fail_assertion "Expected a valid JSON response. Response body: $HTTP_BODY"
}

assert_json_string() {
  local field=$1
  local expected=$2
  jq -e --arg field "$field" --arg expected "$expected" \
    '.[$field] != null and (.[$field] | tostring) == $expected' \
    >/dev/null 2>&1 <<<"$HTTP_BODY" || \
    fail_assertion "Expected JSON '$field' to be '$expected'. Response body: $HTTP_BODY"
}

assert_json_number() {
  local field=$1
  local expected=$2
  jq -e --arg field "$field" --argjson expected "$expected" \
    '.[$field] == $expected' >/dev/null 2>&1 <<<"$HTTP_BODY" || \
    fail_assertion "Expected JSON '$field' to be $expected. Response body: $HTTP_BODY"
}

assert_json_error() {
  local expected_status=$1
  local expected_error=$2
  local expected_message=$3
  assert_status "$expected_status"
  assert_json_content_type
  assert_json_string error "$expected_error"
  assert_json_string message "$expected_message"
}

assert_accepted() {
  local expected_measurements=$1
  assert_status 202
  assert_json_content_type
  assert_json_string status accepted
  assert_json_number accepted_measurements "$expected_measurements"
}

measurement_json() {
  local offset=$1
  local heart_rate=$2
  local timestamp
  timestamp="$(rfc3339_at "$offset")"
  jq -cn --arg timestamp "$timestamp" --argjson heart_rate "$heart_rate" \
    '{timestamp: $timestamp, heart_rate: $heart_rate}'
}

batch_json() {
  local device_id=$1
  local patient_id=$2
  local batch_id=$3
  local measurements=$4
  jq -cn \
    --arg device_id "$device_id" \
    --arg patient_id "$patient_id" \
    --arg batch_id "$batch_id" \
    --argjson measurements "$measurements" \
    '{device_id: $device_id, patient_id: $patient_id, batch_id: $batch_id, measurements: $measurements}'
}

measurements_from_rates() {
  local start_offset=$1
  shift
  local result='[]'
  local index=0 heart_rate measurement
  for heart_rate in "$@"; do
    measurement="$(measurement_json "$((start_offset + index))" "$heart_rate")"
    result="$(jq -cn --argjson current "$result" --argjson item "$measurement" '$current + [$item]')"
    ((index += 1))
  done
  printf '%s' "$result"
}

LAST_WAIT_ERROR=""
LAST_WAIT_OUTPUT=""

wait_until() {
  local name=$1
  local probe=$2
  local deadline now output probe_exit attempt probe_log
  shift 2
  deadline=$(( $(date -u +%s) + TIMEOUT_SECONDS ))
  probe_log="$(mktemp "$TEMP_DIR/wait-probe.XXXXXX")"
  LAST_WAIT_ERROR="probe did not pass"
  LAST_WAIT_OUTPUT=""
  attempt=0

  while :; do
    ((attempt += 1))
    set +e
    (set -Eeuo pipefail; "$probe" "$attempt" "$@") >"$probe_log" 2>&1
    probe_exit=$?
    set -e
    output="$(<"$probe_log")"
    if ((probe_exit == 0)); then
      LAST_WAIT_OUTPUT=$output
      return 0
    fi
    [[ -n "$output" ]] && LAST_WAIT_ERROR=$output
    now=$(date -u +%s)
    if ((now >= deadline)); then
      fail_assertion "Timed out waiting for $name after ${TIMEOUT_SECONDS}s. Last error: $LAST_WAIT_ERROR"
      return 1
    fi
    sleep 1
  done
}

run_test() {
  local name=$1
  shift
  local test_exit
  printf '[TEST] %s\n' "$name"
  test_exit=0
  set +e
  (set -Eeuo pipefail; "$@")
  test_exit=$?
  set -e
  if ((test_exit == 0)); then
    ((PASSED += 1))
    printf '[PASS] %s\n' "$name"
  else
    ((FAILED += 1))
    printf '[FAIL] %s\n' "$name" >&2
  fi
}

skip_test() {
  local name=$1
  local reason=$2
  ((SKIPPED += 1))
  printf '[SKIP] %s: %s\n' "$name" "$reason"
}

probe_ingestion_health() {
  http_request GET "$INGESTION_HEALTH_URL"
  assert_status 200
  assert_json_content_type
  assert_json_string status ok
}

test_ingestion_health() {
  wait_until "ingestion /health" probe_ingestion_health
}

probe_processing_health() {
  http_request GET "$PROCESSING_HEALTH_URL"
  assert_status 200
  assert_json_content_type
  assert_json_string status ok
  assert_json_string service processing
}

test_processing_health() {
  wait_until "processing /health" probe_processing_health
}

test_processing_metrics() {
  local content_type
  http_request GET "$PROCESSING_METRICS_URL"
  assert_status 200
  content_type="$(get_response_header content-type)" || content_type=""
  [[ "${content_type,,}" == *text/plain* ]] || fail_assertion "Expected text/plain Content-Type, got '$content_type'"
  [[ "$HTTP_BODY" == *processing_kafka_messages_total* ]] || \
    fail_assertion "Expected processing_kafka_messages_total in /metrics. Response body: $HTTP_BODY"
}

probe_kafka_publishing() {
  local attempt=$1
  local measurements body
  measurements="$(measurements_from_rates 60 75)"
  body="$(batch_json \
    "e2e-warmup-device-$RUN_ID-$attempt" \
    "e2e-warmup-patient-$RUN_ID" \
    "e2e-warmup-batch-$RUN_ID-$attempt" \
    "$measurements")"
  http_request POST "$INGESTION_TELEMETRY_URL" "$body"
  if [[ "$HTTP_STATUS" == 202 ]]; then
    assert_accepted 1
    return 0
  fi
  if [[ "$HTTP_STATUS" == 503 ]]; then
    fail_assertion "publisher is not ready yet: HTTP 503. Response body: $HTTP_BODY"
    return 1
  fi
  fail_assertion "Unexpected publisher warm-up status HTTP $HTTP_STATUS. Response body: $HTTP_BODY"
}

test_kafka_publishing() {
  wait_until "ingestion Kafka publisher" probe_kafka_publishing
}

test_empty_alerts() {
  http_request GET "$(join_url "$PROCESSING_BASE_URL" "/alerts/e2e-empty-patient-$RUN_ID")"
  assert_status 200
  assert_json_content_type
  jq -e 'type == "array" and length == 0' >/dev/null 2>&1 <<<"$HTTP_BODY" || \
    fail_assertion "Expected an empty JSON array. Response body: $HTTP_BODY"
}

test_alerts_collection() {
  http_request GET "$ALERTS_URL"
  assert_status 200
  assert_json_content_type
  jq -e 'type == "array"' >/dev/null 2>&1 <<<"$HTTP_BODY" || \
    fail_assertion "Expected /alerts to return a JSON array. Response body: $HTTP_BODY"
}

test_processing_rejects_telemetry_post() {
  http_request POST "$(join_url "$PROCESSING_BASE_URL" /api/v1/telemetry)" '{}'
  assert_not_success
}

test_ingestion_has_no_alerts() {
  http_request GET "$(join_url "$INGESTION_BASE_URL" /alerts)"
  assert_not_success
}

test_ingestion_get_telemetry_not_route() {
  http_request GET "$INGESTION_TELEMETRY_URL"
  assert_status_in 404 405
}

test_validation_case() {
  local body=$1
  local expected_message=$2
  http_request POST "$INGESTION_TELEMETRY_URL" "$body"
  assert_json_error 400 invalid_batch "$expected_message"
}

MAX_DEVICE="e2e-max-device-$RUN_ID"
MAX_PATIENT="e2e-max-patient-$RUN_ID"
MAX_BATCH="e2e-max-batch-$RUN_ID"
MAX_BODY=""

test_max_batch() {
  http_request POST "$INGESTION_TELEMETRY_URL" "$MAX_BODY"
  assert_accepted 10
}

test_duplicate_batch() {
  http_request POST "$INGESTION_TELEMETRY_URL" "$MAX_BODY"
  assert_status 200
  assert_json_content_type
  assert_json_string status duplicate_ignored
}

probe_max_telemetry() {
  http_request GET "$(join_url "$PROCESSING_TELEMETRY_URL" "$MAX_PATIENT")"
  assert_status 200
  assert_json_content_type
  jq -e --arg patient "$MAX_PATIENT" '
    type == "array" and
    length == 10 and
    all(.[]; .patient_id == $patient) and
    ([.[].heart_rate] | sort == [70,71,72,73,74,75,76,77,78,79])
  ' >/dev/null 2>&1 <<<"$HTTP_BODY" || \
    fail_assertion "Expected exactly 10 unique max-batch telemetry records with heart rates 70..79. Response body: $HTTP_BODY"
}

test_max_telemetry_saved_once() {
  wait_until "10 telemetry records for $MAX_PATIENT" probe_max_telemetry
}

test_rate_limit() {
  local device patient first_body limited_body measurements retry_after
  device="e2e-rate-device-$RUN_ID"
  patient="e2e-rate-patient-$RUN_ID"
  measurements="$(measurements_from_rates 360 81)"
  first_body="$(batch_json "$device" "$patient" "e2e-rate-batch-$RUN_ID-1" "$measurements")"
  measurements="$(measurements_from_rates 361 82)"
  limited_body="$(batch_json "$device" "$patient" "e2e-rate-batch-$RUN_ID-2" "$measurements")"

  http_request POST "$INGESTION_TELEMETRY_URL" "$first_body"
  assert_accepted 1
  http_request POST "$INGESTION_TELEMETRY_URL" "$limited_body"
  assert_json_error 429 rate_limit_exceeded "device rate limit exceeded"
  retry_after="$(get_response_header retry-after)" || retry_after=""
  [[ "$retry_after" =~ ^[1-9][0-9]*$ ]] || \
    fail_assertion "Expected Retry-After with a positive integer, got '$retry_after'"
}

ALERT_DEVICE="e2e-alert-device-$RUN_ID"
ALERT_PATIENT="e2e-alert-patient-$RUN_ID"
ALERT_BATCH="e2e-alert-batch-$RUN_ID-1"
ALERT_TRIGGERED_AT="$(rfc3339_at 660)"
ALERT_BODY=""
PATIENT_ALERTS_URL="$(join_url "$ALERTS_URL" "$ALERT_PATIENT")"

probe_high_heart_rate_alert() {
  http_request GET "$PATIENT_ALERTS_URL"
  assert_status 200
  assert_json_content_type
  jq -ce \
    --arg patient "$ALERT_PATIENT" \
    --arg triggered_at "$ALERT_TRIGGERED_AT" '
      .[] |
      select(
        .patient_id == $patient and
        .type == "HIGH_HEART_RATE" and
        .message == "Patient has high heart rate" and
        .triggered_at == $triggered_at
      )
    ' <<<"$HTTP_BODY"
}

test_high_heart_rate_alert() {
  http_request POST "$INGESTION_TELEMETRY_URL" "$ALERT_BODY"
  assert_accepted 2
  wait_until "HIGH_HEART_RATE alert for $ALERT_PATIENT" probe_high_heart_rate_alert
}

test_global_alerts() {
  http_request GET "$ALERTS_URL"
  assert_status 200
  assert_json_content_type
  jq -e --arg patient "$ALERT_PATIENT" --arg triggered_at "$ALERT_TRIGGERED_AT" '
    any(.[]; .patient_id == $patient and .type == "HIGH_HEART_RATE" and .triggered_at == $triggered_at)
  ' >/dev/null 2>&1 <<<"$HTTP_BODY" || \
    fail_assertion "Expected /alerts to include the current run alert. Response body: $HTTP_BODY"
}

probe_dedup_telemetry_complete() {
  http_request GET "$(join_url "$PROCESSING_TELEMETRY_URL" "$ALERT_PATIENT")"
  assert_status 200
  assert_json_content_type
  jq -e --arg patient "$ALERT_PATIENT" '
    type == "array" and
    length == 5 and
    all(.[]; .patient_id == $patient) and
    ([.[].heart_rate] | sort == [80,130,130,130,130])
  ' >/dev/null 2>&1 <<<"$HTTP_BODY" || \
    fail_assertion "Expected four high-heart-rate records plus the ordering sentinel. Response body: $HTTP_BODY"
}

test_alert_deduplication() {
  local before_count after_count body measurements
  http_request GET "$PATIENT_ALERTS_URL"
  assert_status 200
  assert_json_content_type
  before_count="$(jq --arg patient "$ALERT_PATIENT" '[.[] | select(.patient_id == $patient and .type == "HIGH_HEART_RATE")] | length' <<<"$HTTP_BODY")"
  [[ "$before_count" == 1 ]] || fail_assertion "Expected one alert before dedup test, got $before_count. Response body: $HTTP_BODY"

  measurements="$(jq -cn \
    --argjson first "$(measurement_json 720 130)" \
    --argjson second "$(measurement_json 780 130)" \
    '[$first, $second]')"
  body="$(batch_json \
    "e2e-alert-dedup-device-$RUN_ID" "$ALERT_PATIENT" \
    "e2e-alert-dedup-batch-$RUN_ID-2" "$measurements")"
  http_request POST "$INGESTION_TELEMETRY_URL" "$body"
  assert_accepted 2

  # A normal event for the same patient is a same-partition ordering sentinel.
  measurements="$(measurements_from_rates 781 80)"
  body="$(batch_json \
    "e2e-alert-sentinel-device-$RUN_ID" "$ALERT_PATIENT" \
    "e2e-alert-sentinel-batch-$RUN_ID-3" "$measurements")"
  http_request POST "$INGESTION_TELEMETRY_URL" "$body"
  assert_accepted 1

  wait_until "dedup batch and ordering sentinel telemetry" probe_dedup_telemetry_complete

  http_request GET "$PATIENT_ALERTS_URL"
  assert_status 200
  assert_json_content_type
  after_count="$(jq --arg patient "$ALERT_PATIENT" '[.[] | select(.patient_id == $patient and .type == "HIGH_HEART_RATE")] | length' <<<"$HTTP_BODY")"
  [[ "$after_count" == "$before_count" ]] || \
    fail_assertion "Alert deduplication failed: count changed from $before_count to $after_count. Response body: $HTTP_BODY"
}

docker_compose_available() {
  command -v docker >/dev/null 2>&1 || return 1
  (cd "$REPO_ROOT" && docker compose version >/dev/null 2>&1) || return 1
  local kafka_id
  kafka_id="$(cd "$REPO_ROOT" && docker compose ps --status running -q kafka 2>/dev/null)" || return 1
  [[ -n "$kafka_id" ]]
}

kafka_produce_keyed() {
  local key=$1
  local value=$2
  printf '%s|%s\n' "$key" "$value" | (
    cd "$REPO_ROOT"
    MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' docker compose exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:9092 \
      --topic telemetry.raw \
      --property parse.key=true \
      --property 'key.separator=|'
  )
}

DLQ_EXPECTED_BASE64=""
probe_current_dlq_message() {
  local output found
  output="$(
    cd "$REPO_ROOT"
    MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' docker compose exec -T kafka \
      /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 \
      --topic telemetry.dlq \
      --from-beginning \
      --timeout-ms 2000 2>/dev/null || true
  )"
  found="$(jq -Rsc --arg payload "$DLQ_EXPECTED_BASE64" '
    split("\n") |
    map(fromjson? | select(type == "object" and .payload_base64 == $payload)) |
    last // empty
  ' <<<"$output")"
  [[ -n "$found" ]] || fail_assertion "DLQ message for the current RUN_ID is not visible yet"
  jq -e --arg payload "$DLQ_EXPECTED_BASE64" '
    (.reason | type == "string" and length > 0) and
    (.timestamp | type == "string" and length > 0) and
    (.source_topic == "telemetry.raw") and
    (.source_partition | type == "number" and . >= 0) and
    (.source_offset | type == "number" and . >= 0) and
    (.payload_base64 == $payload)
  ' >/dev/null 2>&1 <<<"$found" || \
    fail_assertion "DLQ diagnostic envelope is incomplete: $found"
  printf '%s' "$found"
}

DLQ_RECOVERY_PATIENT="e2e-dlq-recovery-patient-$RUN_ID"
probe_dlq_recovery_telemetry() {
  http_request GET "$(join_url "$PROCESSING_TELEMETRY_URL" "$DLQ_RECOVERY_PATIENT")"
  assert_status 200
  assert_json_content_type
  jq -e --arg patient "$DLQ_RECOVERY_PATIENT" --arg run "$RUN_ID" '
    type == "array" and
    length == 1 and
    .[0].patient_id == $patient and
    .[0].event_id == ("e2e-dlq-recovery-event-" + $run) and
    .[0].heart_rate == 88
  ' >/dev/null 2>&1 <<<"$HTTP_BODY" || \
    fail_assertion "Valid Kafka event after poison message has not been processed. Response body: $HTTP_BODY"
}

test_dlq_and_consumer_recovery() {
  local valid_poison poison_payload recovery_event dlq_envelope
  valid_poison="$(jq -cn --arg run_id "$RUN_ID" '{broken: $run_id}')"
  poison_payload=${valid_poison%?}
  DLQ_EXPECTED_BASE64="$(printf '%s' "$poison_payload" | jq -Rrs '@base64')"

  recovery_event="$(jq -cn \
    --arg event_id "e2e-dlq-recovery-event-$RUN_ID" \
    --arg device_id "e2e-dlq-recovery-device-$RUN_ID" \
    --arg patient_id "$DLQ_RECOVERY_PATIENT" \
    --arg timestamp "$(rfc3339_at 900)" \
    --argjson heart_rate 88 '
      {
        event_id: $event_id,
        device_id: $device_id,
        patient_id: $patient_id,
        timestamp: $timestamp,
        heart_rate: $heart_rate
      }
    ')"

  # Identical keys force poison and recovery records into the same Kafka partition.
  kafka_produce_keyed "$DLQ_RECOVERY_PATIENT" "$poison_payload"
  kafka_produce_keyed "$DLQ_RECOVERY_PATIENT" "$recovery_event"

  wait_until "current-run poison message in telemetry.dlq" probe_current_dlq_message
  dlq_envelope=$LAST_WAIT_OUTPUT
  [[ -n "$dlq_envelope" ]] || fail_assertion "Current-run DLQ envelope was empty"
  wait_until "consumer progress after poison message" probe_dlq_recovery_telemetry
}

if ((USE_COMPOSE)); then
  printf '[SETUP] docker compose up -d --build\n'
  (cd "$REPO_ROOT" && docker compose up -d --build) || die "docker compose up failed"
fi

printf '[INFO] run_id=%s\n' "$RUN_ID"
printf '[INFO] repository=%s\n' "$REPO_ROOT"
printf '[INFO] ingestion=%s\n' "$INGESTION_BASE_URL"
printf '[INFO] processing=%s\n' "$PROCESSING_BASE_URL"
printf '[INFO] timeout=%ss\n' "$TIMEOUT_SECONDS"

run_test "ingestion health is ok" test_ingestion_health
run_test "processing health is ok" test_processing_health
run_test "processing exposes prometheus metrics" test_processing_metrics
run_test "kafka publishing path is ready" test_kafka_publishing
run_test "processing returns empty array for a patient without alerts" test_empty_alerts
run_test "processing exposes alerts collection endpoint" test_alerts_collection
run_test "processing does not accept telemetry HTTP API" test_processing_rejects_telemetry_post
run_test "ingestion does not expose alerts endpoint" test_ingestion_has_no_alerts

VALID_MEASUREMENT="$(measurement_json 120 80)"
VALID_MEASUREMENTS="$(jq -cn --argjson item "$VALID_MEASUREMENT" '[$item]')"
VALID_BODY="$(batch_json \
  "e2e-invalid-device-$RUN_ID" "e2e-invalid-patient-$RUN_ID" \
  "e2e-invalid-batch-$RUN_ID-valid-template" "$VALID_MEASUREMENTS")"

run_test "ingestion rejects empty body" test_validation_case "" "invalid JSON body"
run_test "ingestion rejects malformed JSON" test_validation_case '{"device_id":' "invalid JSON body"

UNKNOWN_FIELD_BODY="$(jq -cn \
  --arg device_id "e2e-invalid-device-$RUN_ID" \
  --arg patient_id "e2e-invalid-patient-$RUN_ID" \
  --arg batch_id "e2e-invalid-batch-$RUN_ID-unknown" \
  --argjson measurements "$VALID_MEASUREMENTS" '
  {device_id: $device_id, patient_id: $patient_id, batch_id: $batch_id, unexpected: "field", measurements: $measurements}')"
run_test "ingestion rejects unknown top-level field" test_validation_case "$UNKNOWN_FIELD_BODY" "invalid JSON body"
run_test "ingestion rejects trailing JSON value" test_validation_case "${VALID_BODY}{}" "invalid JSON body"

MISSING_DEVICE_BODY="$(jq -cn \
  --arg patient_id "e2e-invalid-patient-$RUN_ID" \
  --arg batch_id "e2e-invalid-batch-$RUN_ID-no-device" \
  --argjson measurements "$VALID_MEASUREMENTS" \
  '{patient_id: $patient_id, batch_id: $batch_id, measurements: $measurements}')"
run_test "ingestion rejects missing device_id" test_validation_case "$MISSING_DEVICE_BODY" "device_id is required"

MISSING_PATIENT_BODY="$(jq -cn \
  --arg device_id "e2e-invalid-device-$RUN_ID" \
  --arg batch_id "e2e-invalid-batch-$RUN_ID-no-patient" \
  --argjson measurements "$VALID_MEASUREMENTS" \
  '{device_id: $device_id, batch_id: $batch_id, measurements: $measurements}')"
run_test "ingestion rejects missing patient_id" test_validation_case "$MISSING_PATIENT_BODY" "patient_id is required"

MISSING_BATCH_BODY="$(jq -cn \
  --arg device_id "e2e-invalid-device-$RUN_ID" \
  --arg patient_id "e2e-invalid-patient-$RUN_ID" \
  --argjson measurements "$VALID_MEASUREMENTS" \
  '{device_id: $device_id, patient_id: $patient_id, measurements: $measurements}')"
run_test "ingestion rejects missing batch_id" test_validation_case "$MISSING_BATCH_BODY" "batch_id is required"

EMPTY_MEASUREMENTS_BODY="$(batch_json \
  "e2e-invalid-device-$RUN_ID" "e2e-invalid-patient-$RUN_ID" \
  "e2e-invalid-batch-$RUN_ID-empty" '[]')"
run_test "ingestion rejects empty measurements" test_validation_case "$EMPTY_MEASUREMENTS_BODY" "measurements length must be between 1 and 10"

TOO_MANY_MEASUREMENTS="$(measurements_from_rates 180 80 81 82 83 84 85 86 87 88 89 90)"
TOO_MANY_BODY="$(batch_json \
  "e2e-invalid-device-$RUN_ID" "e2e-invalid-patient-$RUN_ID" \
  "e2e-invalid-batch-$RUN_ID-too-many" "$TOO_MANY_MEASUREMENTS")"
run_test "ingestion rejects more than 10 measurements" test_validation_case "$TOO_MANY_BODY" "measurements length must be between 1 and 10"

MISSING_TIMESTAMP_BODY="$(jq -cn \
  --arg device_id "e2e-invalid-device-$RUN_ID" \
  --arg patient_id "e2e-invalid-patient-$RUN_ID" \
  --arg batch_id "e2e-invalid-batch-$RUN_ID-no-timestamp" '
  {device_id: $device_id, patient_id: $patient_id, batch_id: $batch_id, measurements: [{heart_rate: 80}]}')"
run_test "ingestion rejects missing measurement timestamp" test_validation_case "$MISSING_TIMESTAMP_BODY" "measurement timestamp is required"

LOW_HR_MEASUREMENTS="$(jq -cn --argjson item "$(measurement_json 120 19)" '[$item]')"
LOW_HR_BODY="$(batch_json \
  "e2e-invalid-device-$RUN_ID" "e2e-invalid-patient-$RUN_ID" \
  "e2e-invalid-batch-$RUN_ID-low-hr" "$LOW_HR_MEASUREMENTS")"
run_test "ingestion rejects heart_rate below lower bound" test_validation_case "$LOW_HR_BODY" "heart_rate must be greater than or equal to 20"

HIGH_HR_MEASUREMENTS="$(jq -cn --argjson item "$(measurement_json 120 251)" '[$item]')"
HIGH_HR_BODY="$(batch_json \
  "e2e-invalid-device-$RUN_ID" "e2e-invalid-patient-$RUN_ID" \
  "e2e-invalid-batch-$RUN_ID-high-hr" "$HIGH_HR_MEASUREMENTS")"
run_test "ingestion rejects heart_rate above upper bound" test_validation_case "$HIGH_HR_BODY" "heart_rate must be less than or equal to 250"

OUT_OF_ORDER_MEASUREMENTS="$(jq -cn \
  --argjson first "$(measurement_json 121 80)" \
  --argjson second "$(measurement_json 120 81)" \
  '[$first, $second]')"
OUT_OF_ORDER_BODY="$(batch_json \
  "e2e-invalid-device-$RUN_ID" "e2e-invalid-patient-$RUN_ID" \
  "e2e-invalid-batch-$RUN_ID-time-order" "$OUT_OF_ORDER_MEASUREMENTS")"
run_test "ingestion rejects timestamps out of order" test_validation_case "$OUT_OF_ORDER_BODY" "measurement timestamps must be strictly increasing"

MAX_MEASUREMENTS="$(measurements_from_rates 300 70 71 72 73 74 75 76 77 78 79)"
MAX_BODY="$(batch_json "$MAX_DEVICE" "$MAX_PATIENT" "$MAX_BATCH" "$MAX_MEASUREMENTS")"
run_test "ingestion accepts a maximum-size batch" test_max_batch
run_test "ingestion ignores duplicate batch before rate limiting" test_duplicate_batch
run_test "processing stores max batch telemetry exactly once" test_max_telemetry_saved_once
run_test "ingestion rate limits a new batch from the same device" test_rate_limit

ALERT_MEASUREMENTS="$(jq -cn \
  --argjson first "$(measurement_json 600 130)" \
  --argjson second "$(measurement_json 660 130)" \
  '[$first, $second]')"
ALERT_BODY="$(batch_json "$ALERT_DEVICE" "$ALERT_PATIENT" "$ALERT_BATCH" "$ALERT_MEASUREMENTS")"
run_test "high heart rate flows from ingestion to processing alert" test_high_heart_rate_alert
run_test "global alerts endpoint includes created alert" test_global_alerts
run_test "processing deduplicates high-heart-rate alerts while storing telemetry" test_alert_deduplication
run_test "GET ingestion telemetry endpoint is not a success route" test_ingestion_get_telemetry_not_route

if docker_compose_available; then
  run_test "invalid Kafka message is written to DLQ and consumer continues" test_dlq_and_consumer_recovery
else
  skip_test "invalid Kafka message is written to DLQ and consumer continues" "DLQ test requires Docker Compose"
fi

print_summary
SUMMARY_ENABLED=0

if ((FAILED > 0)); then
  printf '[FAIL] E2E tests failed\n' >&2
  exit 1
fi

printf '[OK] E2E tests passed\n'
exit 0
