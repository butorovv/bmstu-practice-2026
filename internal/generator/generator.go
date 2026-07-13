package generator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

type Generator struct {
	cfg    Config
	logger *slog.Logger
}

func New(cfg Config, logger *slog.Logger) *Generator {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Generator{cfg: cfg, logger: logger}
}

func (g *Generator) Run(ctx context.Context) (Result, error) {
	startedAt := time.Now().UTC()
	runID := startedAt.Format("20060102T150405.000000000Z")
	eventLog, err := newEventLog(g.cfg.Output)
	if err != nil {
		return Result{}, err
	}

	stats := &runStats{}
	client := newIngestionClient(g.cfg)
	defer client.Close()
	pacer := newRequestPacer(g.cfg.RPSLimit)
	workersCount := workerCount(g.cfg)
	workers := make([]*deviceWorker, workersCount)
	for index := range workers {
		workers[index] = newDeviceWorker(g.cfg, runID, index, client, pacer, stats, eventLog)
	}

	g.logger.Info(
		"load generation started",
		"mode", g.cfg.Mode,
		"base_devices", g.cfg.Devices,
		"peak_devices", workersCount,
		"patients", g.cfg.Patients,
		"duration", g.cfg.Duration,
		"target_url", g.cfg.TargetURL,
	)

	generationCtx, stopGeneration := context.WithTimeout(ctx, g.cfg.Duration)
	defer stopGeneration()
	deliveryCtx, stopDelivery := context.WithCancel(context.Background())
	defer stopDelivery()

	var producerWG sync.WaitGroup
	var deliveryWG sync.WaitGroup
	producerWG.Add(len(workers))
	deliveryWG.Add(len(workers))
	for _, worker := range workers {
		go func() {
			defer producerWG.Done()
			worker.Produce(generationCtx, startedAt, workersCount)
		}()
		go func() {
			defer deliveryWG.Done()
			worker.Deliver(deliveryCtx)
		}()
	}

	<-generationCtx.Done()
	producerWG.Wait()
	generationDuration := time.Since(startedAt)

	delivered := make(chan struct{})
	go func() {
		deliveryWG.Wait()
		close(delivered)
	}()

	drainTimer := time.NewTimer(g.cfg.DrainTimeout)
	select {
	case <-delivered:
		if !drainTimer.Stop() {
			<-drainTimer.C
		}
	case <-drainTimer.C:
		stopDelivery()
		<-delivered
	}

	var buffered uint64
	for _, worker := range workers {
		buffered += uint64(worker.buffer.Len())
	}

	closeErr := eventLog.Close()
	finishedAt := time.Now().UTC()
	result := stats.result(
		g.cfg,
		runID,
		startedAt,
		finishedAt,
		generationDuration,
		buffered,
		eventLog.path,
	)
	writeErr := writeResult(g.cfg.Output, result)

	g.logger.Info(
		"load generation completed",
		"sent_batches", result.SentBatches,
		"accepted_202", result.Accepted202,
		"duplicates_200", result.Duplicates200,
		"rate_limited_429", result.RateLimited429,
		"unavailable_503", result.Unavailable503,
		"buffered_measurements", result.BufferedMeasurementsLeft,
		"p95_http_latency_ms", result.P95HTTPLatencyMS,
		"throughput_measurements_per_sec", result.ThroughputMeasurements,
		"result", g.cfg.Output,
	)

	if closeErr != nil || writeErr != nil {
		return result, errors.Join(closeErr, writeErr)
	}
	if buffered > 0 {
		return result, fmt.Errorf("drain timeout: %d measurements remain buffered", buffered)
	}
	return result, nil
}
