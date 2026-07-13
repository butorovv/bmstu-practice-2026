package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/metrics"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
	goredis "github.com/redis/go-redis/v9"
)

const DefaultAlertDedupTTL = 5 * time.Minute

type alertDedupRedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.BoolCmd
	Del(ctx context.Context, keys ...string) *goredis.IntCmd
}

type AlertDeduplicator struct {
	client  alertDedupRedisClient
	ttl     time.Duration
	metrics metrics.Recorder
}

var _ usecase.AlertDeduplicator = (*AlertDeduplicator)(nil)

func NewAlertDeduplicator(
	client alertDedupRedisClient,
	ttl time.Duration,
	recorders ...metrics.Recorder,
) *AlertDeduplicator {
	if ttl <= 0 {
		ttl = DefaultAlertDedupTTL
	}
	var recorder metrics.Recorder
	if len(recorders) > 0 {
		recorder = recorders[0]
	}

	return &AlertDeduplicator{
		client:  client,
		ttl:     ttl,
		metrics: recorder,
	}
}

func (d *AlertDeduplicator) Reserve(
	ctx context.Context,
	patientID string,
	alertType string,
) (bool, error) {
	startedAt := time.Now()
	reserved, err := d.client.SetNX(ctx, alertDedupKey(patientID, alertType), "1", d.ttl).Result()
	d.observeRedis("alert_dedup_reserve", time.Since(startedAt))

	return reserved, err
}

func (d *AlertDeduplicator) Release(
	ctx context.Context,
	patientID string,
	alertType string,
) error {
	return d.client.Del(ctx, alertDedupKey(patientID, alertType)).Err()
}

func alertDedupKey(patientID string, alertType string) string {
	return fmt.Sprintf("processing:alert:dedup:%s:%s", patientID, alertType)
}

func (d *AlertDeduplicator) observeRedis(operation string, duration time.Duration) {
	if d.metrics == nil {
		return
	}

	d.metrics.ObserveHistogram(
		"processing_redis_duration_seconds",
		metrics.Labels{"operation": operation},
		duration.Seconds(),
	)
}
