package generator

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type deliveryOutcome int

const (
	deliveryStopped deliveryOutcome = iota
	deliveryAccepted
	deliveryDuplicate
	deliveryDropped
)

type deviceWorker struct {
	cfg          Config
	runID        string
	index        int
	deviceID     string
	patientID    string
	patientIndex int
	buffer       *measurementBuffer
	client       *ingestionClient
	pacer        *requestPacer
	stats        *runStats
	eventLog     *eventLog
}

func newDeviceWorker(
	cfg Config,
	runID string,
	index int,
	client *ingestionClient,
	pacer *requestPacer,
	stats *runStats,
	eventLog *eventLog,
) *deviceWorker {
	patientIndex := index % cfg.Patients
	return &deviceWorker{
		cfg:          cfg,
		runID:        runID,
		index:        index,
		deviceID:     fmt.Sprintf("device-%06d", index+1),
		patientID:    fmt.Sprintf("patient-%06d", patientIndex+1),
		patientIndex: patientIndex,
		buffer:       newMeasurementBuffer(),
		client:       client,
		pacer:        pacer,
		stats:        stats,
		eventLog:     eventLog,
	}
}

func (d *deviceWorker) Produce(ctx context.Context, startedAt time.Time, workers int) {
	defer d.buffer.Close()

	phase := time.Duration(d.index) * d.cfg.BaseInterval / time.Duration(workers)
	scheduledAt := startedAt.Add(phase)
	timer := time.NewTimer(max(time.Until(scheduledAt), 0))
	defer timer.Stop()

	var sequence uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			elapsed := scheduledAt.Sub(startedAt)
			if d.index < activeDeviceCount(d.cfg, elapsed) {
				sequence++
				d.buffer.Push(Measurement{
					Timestamp: scheduledAt,
					HeartRate: scenarioHeartRate(d.cfg.Mode, d.patientIndex, sequence),
				})
				d.stats.generatedMeasurements.Add(1)
			}
			scheduledAt = scheduledAt.Add(d.cfg.BaseInterval)
			timer.Reset(max(time.Until(scheduledAt), 0))
		}
	}
}

func (d *deviceWorker) Deliver(ctx context.Context) {
	var batchSequence uint64
	for {
		measurements, closed := d.buffer.Snapshot(d.cfg.BatchSize, d.cfg.MaxBatchSize)
		if closed {
			return
		}
		if len(measurements) == 0 {
			if err := d.buffer.Wait(ctx); err != nil {
				return
			}
			continue
		}

		batchSequence++
		batch := TelemetryBatch{
			DeviceID:     d.deviceID,
			PatientID:    d.patientID,
			BatchID:      fmt.Sprintf("%s-%s-%09d", d.deviceID, d.runID, batchSequence),
			Measurements: measurements,
		}

		outcome := d.deliverWithRetry(ctx, batch)
		if outcome == deliveryStopped {
			return
		}
		if outcome == deliveryDropped {
			d.buffer.Remove(len(measurements))
			continue
		}

		d.buffer.Remove(len(measurements))
		d.writeAcceptedBatch(batch)
		if d.cfg.Mode == ModeDuplicate && outcome == deliveryAccepted {
			_ = d.deliverWithRetry(ctx, batch)
		}
	}
}

func (d *deviceWorker) deliverWithRetry(ctx context.Context, batch TelemetryBatch) deliveryOutcome {
	retry := 0
	for {
		if err := d.pacer.Wait(ctx); err != nil {
			return deliveryStopped
		}

		result, err := d.client.Send(ctx, batch)
		d.stats.recordAttempt(len(batch.Measurements), result.latency)
		if err != nil {
			d.stats.failedRequests.Add(1)
			if !waitContext(ctx, d.backoff(retry)) {
				return deliveryStopped
			}
			retry++
			continue
		}

		switch result.statusCode {
		case http.StatusAccepted:
			d.stats.accepted202.Add(1)
			d.stats.acceptedMeasurements.Add(uint64(len(batch.Measurements)))
			return deliveryAccepted
		case http.StatusOK:
			d.stats.duplicates200.Add(1)
			return deliveryDuplicate
		case http.StatusTooManyRequests:
			d.stats.rateLimited429.Add(1)
			delay := result.retryAfter
			if delay <= 0 {
				delay = d.cfg.RetryBackoff
			}
			if !waitContext(ctx, delay) {
				return deliveryStopped
			}
		case http.StatusServiceUnavailable:
			d.stats.unavailable503.Add(1)
			if !waitContext(ctx, d.backoff(retry)) {
				return deliveryStopped
			}
			retry++
		default:
			d.stats.failedRequests.Add(1)
			return deliveryDropped
		}
	}
}

func (d *deviceWorker) backoff(retry int) time.Duration {
	backoff := d.cfg.RetryBackoff
	for i := 0; i < retry && backoff < d.cfg.MaxBackoff; i++ {
		backoff *= 2
		if backoff > d.cfg.MaxBackoff {
			backoff = d.cfg.MaxBackoff
		}
	}
	jitterRange := backoff / 5
	if jitterRange == 0 {
		return backoff
	}
	jitter := time.Duration((d.index+retry)%5) * jitterRange / 5
	return backoff + jitter
}

func (d *deviceWorker) writeAcceptedBatch(batch TelemetryBatch) {
	eventIDs := make([]string, len(batch.Measurements))
	for index := range batch.Measurements {
		eventIDs[index] = fmt.Sprintf("%s-%d", batch.BatchID, index)
	}
	_ = d.eventLog.Write(acceptedBatchRecord{
		BatchID:      batch.BatchID,
		DeviceID:     batch.DeviceID,
		PatientID:    batch.PatientID,
		EventIDs:     eventIDs,
		Measurements: len(batch.Measurements),
		AcceptedAt:   time.Now().UTC(),
	})
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
