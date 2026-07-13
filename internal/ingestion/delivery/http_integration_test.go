package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	redisrepo "github.com/butorovv/bmstu-practice-2026/internal/ingestion/repository/redis"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
	goredis "github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const integrationRedisDB = 15

func TestIngestionKafkaRedisIntegration(t *testing.T) {
	brokersValue := os.Getenv("KAFKA_TEST_BROKERS")
	redisAddr := os.Getenv("REDIS_TEST_ADDR")
	if brokersValue == "" || redisAddr == "" {
		t.Skip("KAFKA_TEST_BROKERS and REDIS_TEST_ADDR are required")
	}

	brokers := splitIntegrationBrokers(brokersValue)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient := redisrepo.NewClient(redisAddr, os.Getenv("REDIS_TEST_PASSWORD"), integrationRedisDB)
	t.Cleanup(func() {
		_ = redisClient.Close()
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}

	kafkaPublisher, err := publisher.NewKafkaPublisherWithConfig(publisher.KafkaPublisherConfig{
		Brokers:        brokers,
		PublishTimeout: 2 * time.Second,
		MaxAttempts:    3,
		MaxInFlight:    4,
	})
	if err != nil {
		t.Fatalf("create Kafka publisher: %v", err)
	}
	t.Cleanup(func() {
		_ = kafkaPublisher.Close()
	})
	if err := kafkaPublisher.Ready(ctx); err != nil {
		t.Fatalf("check Kafka readiness: %v", err)
	}

	idempotency := redisrepo.NewIdempotencyRepository(redisClient)
	rateLimiter := redisrepo.NewRateLimiter(redisClient)
	handler := NewHandlerWithOptions(
		kafkaPublisher,
		validator.New(),
		idempotency,
		rateLimiter,
		HandlerOptions{
			RequestTimeout: 4 * time.Second,
			ReadinessChecks: map[string]ReadinessCheck{
				"kafka": kafkaPublisher.Ready,
				"redis": func(ctx context.Context) error {
					return redisClient.Ping(ctx).Err()
				},
			},
		},
	)
	server := httptest.NewServer(NewRouter(handler))
	t.Cleanup(server.Close)

	t.Run("real dependencies are ready", func(t *testing.T) {
		response, body := integrationRequest(t, server.Client(), http.MethodGet, server.URL+"/ready", nil)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", response.StatusCode, http.StatusOK, body)
		}
	})

	t.Run("publishes once and ignores duplicate batch", func(t *testing.T) {
		testID := integrationTestID()
		deviceID := "integration-duplicate-device-" + testID
		patientID := "integration-duplicate-patient-" + testID
		batchID := "integration-duplicate-batch-" + testID
		cleanupIngestionRedisKeys(t, redisClient, deviceID, batchID)

		before := topicHighWatermarks(t, brokers[0])
		batch := integrationBatch(deviceID, patientID, batchID)

		firstResponse, firstBody := integrationRequest(
			t,
			server.Client(),
			http.MethodPost,
			server.URL+"/api/v1/telemetry",
			batch,
		)
		if firstResponse.StatusCode != http.StatusAccepted {
			t.Fatalf("first status = %d, want %d; body=%s", firstResponse.StatusCode, http.StatusAccepted, firstBody)
		}

		secondResponse, secondBody := integrationRequest(
			t,
			server.Client(),
			http.MethodPost,
			server.URL+"/api/v1/telemetry",
			batch,
		)
		if secondResponse.StatusCode != http.StatusOK {
			t.Fatalf("duplicate status = %d, want %d; body=%s", secondResponse.StatusCode, http.StatusOK, secondBody)
		}
		if status := integrationResponseStatus(t, secondBody); status != "duplicate_ignored" {
			t.Fatalf("duplicate response status = %q, want duplicate_ignored", status)
		}

		after := topicHighWatermarks(t, brokers[0])
		messages := topicMessagesBetween(t, brokers[0], before, after)
		eventID := batchID + "-0"
		matching := matchingIntegrationMessages(t, messages, eventID)
		if len(matching) != 1 {
			t.Fatalf("Kafka messages for event_id=%q = %d, want 1", eventID, len(matching))
		}
		if key := string(matching[0].Key); key != patientID {
			t.Fatalf("Kafka key = %q, want %q", key, patientID)
		}

		var event publisher.TelemetryEvent
		if err := json.Unmarshal(matching[0].Value, &event); err != nil {
			t.Fatalf("decode Kafka event: %v", err)
		}
		if event.EventID != eventID || event.DeviceID != deviceID || event.PatientID != patientID {
			t.Fatalf("Kafka event = %+v", event)
		}

		ttl, err := redisClient.TTL(context.Background(), "idempotency:"+deviceID+":"+batchID).Result()
		if err != nil {
			t.Fatalf("read idempotency TTL: %v", err)
		}
		if ttl <= 23*time.Hour || ttl > redisrepo.IdempotencyTTL {
			t.Fatalf("idempotency TTL = %v, want close to %v", ttl, redisrepo.IdempotencyTTL)
		}
	})

	t.Run("rejects rate limit excess and releases batch reservation", func(t *testing.T) {
		testID := integrationTestID()
		deviceID := "integration-rate-device-" + testID
		patientID := "integration-rate-patient-" + testID
		firstBatchID := "integration-rate-first-" + testID
		secondBatchID := "integration-rate-second-" + testID
		cleanupIngestionRedisKeys(t, redisClient, deviceID, firstBatchID, secondBatchID)
		before := topicHighWatermarks(t, brokers[0])

		firstResponse, firstBody := integrationRequest(
			t,
			server.Client(),
			http.MethodPost,
			server.URL+"/api/v1/telemetry",
			integrationBatch(deviceID, patientID, firstBatchID),
		)
		if firstResponse.StatusCode != http.StatusAccepted {
			t.Fatalf("first status = %d, want %d; body=%s", firstResponse.StatusCode, http.StatusAccepted, firstBody)
		}

		secondResponse, secondBody := integrationRequest(
			t,
			server.Client(),
			http.MethodPost,
			server.URL+"/api/v1/telemetry",
			integrationBatch(deviceID, patientID, secondBatchID),
		)
		if secondResponse.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("second status = %d, want %d; body=%s", secondResponse.StatusCode, http.StatusTooManyRequests, secondBody)
		}
		if secondResponse.Header.Get("Retry-After") == "" {
			t.Fatal("Retry-After header is empty")
		}
		if code := integrationErrorCode(t, secondBody); code != "rate_limit_exceeded" {
			t.Fatalf("error code = %q, want rate_limit_exceeded", code)
		}

		exists, err := redisClient.Exists(
			context.Background(),
			"idempotency:"+deviceID+":"+secondBatchID,
		).Result()
		if err != nil {
			t.Fatalf("check released idempotency key: %v", err)
		}
		if exists != 0 {
			t.Fatal("rate-limited batch idempotency key was not released")
		}

		after := topicHighWatermarks(t, brokers[0])
		messages := topicMessagesBetween(t, brokers[0], before, after)
		if count := len(matchingIntegrationMessages(t, messages, firstBatchID+"-0")); count != 1 {
			t.Fatalf("first batch Kafka messages = %d, want 1", count)
		}
		if count := len(matchingIntegrationMessages(t, messages, secondBatchID+"-0")); count != 0 {
			t.Fatalf("rate-limited batch Kafka messages = %d, want 0", count)
		}
	})

	t.Run("returns service unavailable when broker is unavailable", func(t *testing.T) {
		testID := integrationTestID()
		deviceID := "integration-broker-device-" + testID
		patientID := "integration-broker-patient-" + testID
		batchID := "integration-broker-batch-" + testID
		cleanupIngestionRedisKeys(t, redisClient, deviceID, batchID)

		unavailableBroker := closedLocalAddress(t)
		unavailablePublisher, err := publisher.NewKafkaPublisherWithConfig(publisher.KafkaPublisherConfig{
			Brokers:        []string{unavailableBroker},
			PublishTimeout: 300 * time.Millisecond,
			MaxAttempts:    1,
			MaxInFlight:    1,
		})
		if err != nil {
			t.Fatalf("create unavailable Kafka publisher: %v", err)
		}
		t.Cleanup(func() {
			_ = unavailablePublisher.Close()
		})

		unavailableHandler := NewHandlerWithOptions(
			unavailablePublisher,
			validator.New(),
			idempotency,
			rateLimiter,
			HandlerOptions{RequestTimeout: time.Second},
		)
		unavailableServer := httptest.NewServer(NewRouter(unavailableHandler))
		t.Cleanup(unavailableServer.Close)

		response, body := integrationRequest(
			t,
			unavailableServer.Client(),
			http.MethodPost,
			unavailableServer.URL+"/api/v1/telemetry",
			integrationBatch(deviceID, patientID, batchID),
		)
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d; body=%s", response.StatusCode, http.StatusServiceUnavailable, body)
		}
		if code := integrationErrorCode(t, body); code != "publisher_unavailable" {
			t.Fatalf("error code = %q, want publisher_unavailable", code)
		}

		exists, err := redisClient.Exists(
			context.Background(),
			"idempotency:"+deviceID+":"+batchID,
		).Result()
		if err != nil {
			t.Fatalf("check released idempotency key: %v", err)
		}
		if exists != 0 {
			t.Fatal("failed publication idempotency key was not released")
		}
	})
}

func integrationBatch(deviceID string, patientID string, batchID string) usecase.TelemetryBatch {
	return usecase.TelemetryBatch{
		DeviceID:  deviceID,
		PatientID: patientID,
		BatchID:   batchID,
		Measurements: []usecase.Measurement{{
			Timestamp: time.Now().UTC().Truncate(time.Millisecond),
			HeartRate: 78,
		}},
	}
}

func integrationRequest(
	t *testing.T,
	client *http.Client,
	method string,
	url string,
	payload any,
) (*http.Response, []byte) {
	t.Helper()

	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("encode request: %v", err)
		}
		body = bytes.NewReader(encoded)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	response, err := client.Do(req)
	if err != nil {
		t.Fatalf("send request: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	return response, responseBody
}

func integrationResponseStatus(t *testing.T, body []byte) string {
	t.Helper()
	var response struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	return response.Status
}

func integrationErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var response struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return response.Error
}

func cleanupIngestionRedisKeys(t *testing.T, client *goredis.Client, deviceID string, batchIDs ...string) {
	t.Helper()
	keys := []string{"rate:" + deviceID}
	for _, batchID := range batchIDs {
		keys = append(keys, "idempotency:"+deviceID+":"+batchID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Del(ctx, keys...).Err(); err != nil {
		t.Fatalf("clean Redis test keys: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, keys...).Err()
	})
}

func topicHighWatermarks(t *testing.T, broker string) map[int]int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metadataConn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial Kafka metadata: %v", err)
	}
	partitions, err := metadataConn.ReadPartitions(publisher.TelemetryRawTopic)
	_ = metadataConn.Close()
	if err != nil {
		t.Fatalf("read Kafka partitions: %v", err)
	}

	offsets := make(map[int]int64, len(partitions))
	for _, partition := range partitions {
		if _, exists := offsets[partition.ID]; exists {
			continue
		}
		leaderConn, err := kafka.DialLeader(ctx, "tcp", broker, publisher.TelemetryRawTopic, partition.ID)
		if err != nil {
			t.Fatalf("dial Kafka partition %d: %v", partition.ID, err)
		}
		_, last, err := leaderConn.ReadOffsets()
		_ = leaderConn.Close()
		if err != nil {
			t.Fatalf("read Kafka partition %d offsets: %v", partition.ID, err)
		}
		offsets[partition.ID] = last
	}

	return offsets
}

func topicMessagesBetween(
	t *testing.T,
	broker string,
	startOffsets map[int]int64,
	endOffsets map[int]int64,
) []kafka.Message {
	t.Helper()

	var messages []kafka.Message
	for partition, start := range startOffsets {
		end := endOffsets[partition]
		if end <= start {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := kafka.DialLeader(ctx, "tcp", broker, publisher.TelemetryRawTopic, partition)
		cancel()
		if err != nil {
			t.Fatalf("dial Kafka partition %d for reading: %v", partition, err)
		}
		if _, err := conn.Seek(start, kafka.SeekAbsolute); err != nil {
			_ = conn.Close()
			t.Fatalf("set Kafka partition %d offset %d: %v", partition, start, err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			_ = conn.Close()
			t.Fatalf("set Kafka partition %d deadline: %v", partition, err)
		}

		for offset := start; offset < end; offset++ {
			message, err := conn.ReadMessage(1 << 20)
			if err != nil {
				_ = conn.Close()
				t.Fatalf("read Kafka partition %d offset %d: %v", partition, offset, err)
			}
			messages = append(messages, message)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close Kafka partition %d reader: %v", partition, err)
		}
	}

	return messages
}

func matchingIntegrationMessages(
	t *testing.T,
	messages []kafka.Message,
	eventID string,
) []kafka.Message {
	t.Helper()

	matching := make([]kafka.Message, 0, 1)
	for _, message := range messages {
		var event publisher.TelemetryEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			continue
		}
		if event.EventID == eventID {
			matching = append(matching, message)
		}
	}
	return matching
}

func splitIntegrationBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func integrationTestID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

func closedLocalAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve unavailable broker address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close unavailable broker address: %v", err)
	}
	return address
}
