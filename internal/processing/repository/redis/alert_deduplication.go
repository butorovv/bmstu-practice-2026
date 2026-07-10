package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
	goredis "github.com/redis/go-redis/v9"
)

const DefaultAlertDedupTTL = 5 * time.Minute

type alertDedupRedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.BoolCmd
	Del(ctx context.Context, keys ...string) *goredis.IntCmd
}

type AlertDeduplicator struct {
	client alertDedupRedisClient
	ttl    time.Duration
}

var _ usecase.AlertDeduplicator = (*AlertDeduplicator)(nil)

func NewAlertDeduplicator(client alertDedupRedisClient, ttl time.Duration) *AlertDeduplicator {
	if ttl <= 0 {
		ttl = DefaultAlertDedupTTL
	}

	return &AlertDeduplicator{
		client: client,
		ttl:    ttl,
	}
}

func (d *AlertDeduplicator) Reserve(
	ctx context.Context,
	patientID string,
	alertType string,
) (bool, error) {
	return d.client.SetNX(ctx, alertDedupKey(patientID, alertType), "1", d.ttl).Result()
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
