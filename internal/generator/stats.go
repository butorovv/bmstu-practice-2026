package generator

import (
	"math"
	"sync/atomic"
	"time"
)

const maxLatencyMilliseconds = 60000

type runStats struct {
	generatedMeasurements atomic.Uint64
	sentBatches           atomic.Uint64
	sentMeasurements      atomic.Uint64
	accepted202           atomic.Uint64
	duplicates200         atomic.Uint64
	rateLimited429        atomic.Uint64
	unavailable503        atomic.Uint64
	failedRequests        atomic.Uint64
	acceptedMeasurements  atomic.Uint64
	latencies             [maxLatencyMilliseconds + 1]atomic.Uint64
}

func (s *runStats) recordAttempt(measurements int, latency time.Duration) {
	s.sentBatches.Add(1)
	s.sentMeasurements.Add(uint64(measurements))
	milliseconds := int(latency.Milliseconds())
	if milliseconds < 0 {
		milliseconds = 0
	}
	if milliseconds > maxLatencyMilliseconds {
		milliseconds = maxLatencyMilliseconds
	}
	s.latencies[milliseconds].Add(1)
}

func (s *runStats) p95Milliseconds() int64 {
	total := s.sentBatches.Load()
	if total == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(total) * 0.95))
	var observed uint64
	for milliseconds := 0; milliseconds <= maxLatencyMilliseconds; milliseconds++ {
		observed += s.latencies[milliseconds].Load()
		if observed >= target {
			return int64(milliseconds)
		}
	}
	return maxLatencyMilliseconds
}

type Result struct {
	RunID                    string    `json:"run_id"`
	Mode                     Mode      `json:"mode"`
	Devices                  int       `json:"devices"`
	PeakDevices              int       `json:"peak_devices"`
	Patients                 int       `json:"patients"`
	DurationSeconds          int64     `json:"duration_seconds"`
	StartedAt                time.Time `json:"started_at"`
	FinishedAt               time.Time `json:"finished_at"`
	GeneratedMeasurements    uint64    `json:"generated_measurements"`
	SentBatches              uint64    `json:"sent_batches"`
	SentMeasurements         uint64    `json:"sent_measurements"`
	Accepted202              uint64    `json:"accepted_202"`
	Duplicates200            uint64    `json:"duplicates_200"`
	RateLimited429           uint64    `json:"rate_limited_429"`
	Unavailable503           uint64    `json:"unavailable_503"`
	FailedRequests           uint64    `json:"failed_requests"`
	AcceptedMeasurements     uint64    `json:"accepted_measurements"`
	BufferedMeasurementsLeft uint64    `json:"buffered_measurements_remaining"`
	P95HTTPLatencyMS         int64     `json:"p95_http_latency_ms"`
	ThroughputMeasurements   float64   `json:"throughput_measurements_per_sec"`
	EventLog                 string    `json:"event_log"`
}

func (s *runStats) result(
	cfg Config,
	runID string,
	startedAt time.Time,
	finishedAt time.Time,
	generationDuration time.Duration,
	buffered uint64,
	eventLog string,
) Result {
	acceptedMeasurements := s.acceptedMeasurements.Load()
	throughput := 0.0
	if generationDuration > 0 {
		throughput = float64(acceptedMeasurements) / generationDuration.Seconds()
	}

	durationSeconds := int64(math.Round(generationDuration.Seconds()))
	if durationSeconds == 0 && generationDuration > 0 {
		durationSeconds = 1
	}

	return Result{
		RunID:                    runID,
		Mode:                     cfg.Mode,
		Devices:                  cfg.Devices,
		PeakDevices:              workerCount(cfg),
		Patients:                 cfg.Patients,
		DurationSeconds:          durationSeconds,
		StartedAt:                startedAt,
		FinishedAt:               finishedAt,
		GeneratedMeasurements:    s.generatedMeasurements.Load(),
		SentBatches:              s.sentBatches.Load(),
		SentMeasurements:         s.sentMeasurements.Load(),
		Accepted202:              s.accepted202.Load(),
		Duplicates200:            s.duplicates200.Load(),
		RateLimited429:           s.rateLimited429.Load(),
		Unavailable503:           s.unavailable503.Load(),
		FailedRequests:           s.failedRequests.Load(),
		AcceptedMeasurements:     acceptedMeasurements,
		BufferedMeasurementsLeft: buffered,
		P95HTTPLatencyMS:         s.p95Milliseconds(),
		ThroughputMeasurements:   throughput,
		EventLog:                 eventLog,
	}
}
