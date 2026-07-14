package generator

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGeneratorSendsContractBatchesAndWritesResults(t *testing.T) {
	var received atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var batch TelemetryBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("decode batch: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if batch.DeviceID == "" || batch.PatientID == "" || batch.BatchID == "" {
			t.Errorf("invalid batch identifiers: %+v", batch)
		}
		if len(batch.Measurements) < 1 || len(batch.Measurements) > 10 {
			t.Errorf("measurement count = %d", len(batch.Measurements))
		}
		received.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	cfg := testConfig()
	cfg.Devices = 1
	cfg.Patients = 1
	cfg.TargetURL = server.URL
	cfg.Output = filepath.Join(t.TempDir(), "run.json")
	result, err := New(cfg, discardLogger()).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if received.Load() == 0 || result.Accepted202 == 0 || result.AcceptedMeasurements == 0 {
		t.Fatalf("result = %+v", result)
	}
	if result.BufferedMeasurementsLeft != 0 {
		t.Fatalf("buffered measurements = %d", result.BufferedMeasurementsLeft)
	}
	if _, err := os.Stat(cfg.Output); err != nil {
		t.Fatalf("result file: %v", err)
	}
	if _, err := os.Stat(result.EventLog); err != nil {
		t.Fatalf("event log: %v", err)
	}
}

func TestGeneratorRetriesSameBatchAfter503And429(t *testing.T) {
	var calls atomic.Uint64
	var mu sync.Mutex
	var batchIDs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var batch TelemetryBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("decode batch: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		batchIDs = append(batchIDs, batch.BatchID)
		mu.Unlock()
		switch calls.Add(1) {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
		case 2:
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	t.Cleanup(server.Close)

	cfg := testConfig()
	cfg.Devices = 1
	cfg.Patients = 1
	cfg.TargetURL = server.URL
	cfg.Output = filepath.Join(t.TempDir(), "retry.json")
	result, err := New(cfg, discardLogger()).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Unavailable503 != 1 || result.RateLimited429 != 1 || result.Accepted202 == 0 {
		t.Fatalf("result = %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(batchIDs) < 3 || batchIDs[0] != batchIDs[1] || batchIDs[1] != batchIDs[2] {
		t.Fatalf("retry batch IDs = %v", batchIDs)
	}
}

func TestGeneratorDuplicateModeRepeatsAcceptedBatchID(t *testing.T) {
	seen := make(map[string]int)
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var batch TelemetryBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("decode batch: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		seen[batch.BatchID]++
		count := seen[batch.BatchID]
		mu.Unlock()
		if count == 1 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	cfg := testConfig()
	cfg.Mode = ModeDuplicate
	cfg.TargetURL = server.URL
	cfg.Output = filepath.Join(t.TempDir(), "duplicate.json")
	result, err := New(cfg, discardLogger()).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Accepted202 == 0 || result.Duplicates200 != result.Accepted202 {
		t.Fatalf("result = %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	for batchID, count := range seen {
		if count != 2 {
			t.Fatalf("batch %s sent %d times, want 2", batchID, count)
		}
	}
}

func TestGeneratorBuffersOfflineMeasurementsAndSendsRecoveryBatch(t *testing.T) {
	startedAt := time.Now()
	var mu sync.Mutex
	var acceptedSizes []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var batch TelemetryBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("decode batch: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if time.Since(startedAt) < 70*time.Millisecond {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		mu.Lock()
		acceptedSizes = append(acceptedSizes, len(batch.Measurements))
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	cfg := testConfig()
	cfg.Devices = 1
	cfg.Patients = 1
	cfg.Duration = 100 * time.Millisecond
	cfg.BaseInterval = 10 * time.Millisecond
	cfg.TargetURL = server.URL
	cfg.Output = filepath.Join(t.TempDir(), "offline.json")
	result, err := New(cfg, discardLogger()).Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Unavailable503 == 0 {
		t.Fatalf("unavailable_503 = %d, want > 0", result.Unavailable503)
	}
	if result.BufferedMeasurementsLeft != 0 || result.AcceptedMeasurements != result.GeneratedMeasurements {
		t.Fatalf("measurements were lost: result=%+v", result)
	}

	mu.Lock()
	defer mu.Unlock()
	foundRecoveryBatch := false
	for _, size := range acceptedSizes {
		if size > 1 {
			foundRecoveryBatch = true
		}
		if size > maxContractBatchSize {
			t.Fatalf("recovery batch size = %d", size)
		}
	}
	if !foundRecoveryBatch {
		t.Fatalf("accepted batch sizes = %v, want a recovery batch", acceptedSizes)
	}
}

func TestDeviceProducesMeasurementsOnExactIntervalGrid(t *testing.T) {
	cfg := testConfig()
	cfg.Devices = 1
	cfg.Duration = 35 * time.Millisecond
	cfg.BaseInterval = 10 * time.Millisecond
	worker := &deviceWorker{
		cfg:    cfg,
		index:  0,
		buffer: newMeasurementBuffer(),
		stats:  &runStats{},
	}
	startedAt := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	worker.Produce(ctx, startedAt, 1)
	measurements, closed := worker.buffer.Snapshot(1, maxContractBatchSize)
	if closed || len(measurements) < 3 {
		t.Fatalf("measurements = %d, closed=%v", len(measurements), closed)
	}
	for index, measurement := range measurements {
		want := startedAt.Add(time.Duration(index) * cfg.BaseInterval)
		if !measurement.Timestamp.Equal(want) {
			t.Fatalf("measurement %d timestamp = %v, want %v", index, measurement.Timestamp, want)
		}
	}
}

func testConfig() Config {
	return Config{
		TargetURL:    "http://localhost/api/v1/telemetry",
		Mode:         ModeNormal,
		Devices:      2,
		Patients:     2,
		Duration:     80 * time.Millisecond,
		BatchSize:    1,
		MaxBatchSize: 10,
		BaseInterval: 20 * time.Millisecond,
		RPSLimit:     10000,
		Output:       "results/test.json",
		HTTPTimeout:  100 * time.Millisecond,
		DrainTimeout: time.Second,
		RetryBackoff: time.Millisecond,
		MaxBackoff:   5 * time.Millisecond,
		SpikeFactor:  5,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
