package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type attemptResult struct {
	statusCode int
	retryAfter time.Duration
	latency    time.Duration
}

type ingestionClient struct {
	targetURL string
	timeout   time.Duration
	client    *http.Client
}

func newIngestionClient(cfg Config) *ingestionClient {
	maxIdleConnections := min(workerCount(cfg), 10000)
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          maxIdleConnections,
		MaxIdleConnsPerHost:   maxIdleConnections,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	return &ingestionClient{
		targetURL: cfg.TargetURL,
		timeout:   cfg.HTTPTimeout,
		client:    &http.Client{Transport: transport},
	}
}

func (c *ingestionClient) Send(ctx context.Context, batch TelemetryBatch) (attemptResult, error) {
	payload, err := json.Marshal(batch)
	if err != nil {
		return attemptResult{}, fmt.Errorf("encode batch: %w", err)
	}

	attemptCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, c.targetURL, bytes.NewReader(payload))
	if err != nil {
		return attemptResult{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	startedAt := time.Now()
	response, err := c.client.Do(req)
	latency := time.Since(startedAt)
	if err != nil {
		return attemptResult{latency: latency}, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))

	return attemptResult{
		statusCode: response.StatusCode,
		retryAfter: parseRetryAfter(response.Header.Get("Retry-After"), time.Now()),
		latency:    latency,
	}, nil
}

func (c *ingestionClient) Close() {
	c.client.CloseIdleConnections()
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil && retryAt.After(now) {
		return retryAt.Sub(now)
	}
	return 0
}
