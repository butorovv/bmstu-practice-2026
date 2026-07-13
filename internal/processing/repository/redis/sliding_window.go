package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/metrics"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
	goredis "github.com/redis/go-redis/v9"
)

const (
	SlidingWindowDuration = time.Minute
	DefaultWindowTTL      = 5 * time.Minute
)

type slidingWindowRedisClient interface {
	ZAdd(ctx context.Context, key string, members ...goredis.Z) *goredis.IntCmd
	ZRangeByScore(ctx context.Context, key string, opt *goredis.ZRangeBy) *goredis.StringSliceCmd
	ZRemRangeByScore(ctx context.Context, key string, min string, max string) *goredis.IntCmd
	HSet(ctx context.Context, key string, values ...interface{}) *goredis.IntCmd
	HDel(ctx context.Context, key string, fields ...string) *goredis.IntCmd
	HMGet(ctx context.Context, key string, fields ...string) *goredis.SliceCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd
}

type SlidingWindowRepository struct {
	client  slidingWindowRedisClient
	ttl     time.Duration
	metrics metrics.Recorder
}

var _ usecase.SlidingWindow = (*SlidingWindowRepository)(nil)

func NewSlidingWindowRepository(
	client slidingWindowRedisClient,
	ttl time.Duration,
	recorders ...metrics.Recorder,
) *SlidingWindowRepository {
	if ttl <= 0 {
		ttl = DefaultWindowTTL
	}
	var recorder metrics.Recorder
	if len(recorders) > 0 {
		recorder = recorders[0]
	}

	return &SlidingWindowRepository{
		client:  client,
		ttl:     ttl,
		metrics: recorder,
	}
}

func (r *SlidingWindowRepository) Add(
	ctx context.Context,
	event model.TelemetryEvent,
) ([]model.TelemetryEvent, error) {
	startedAt := time.Now()
	defer func() {
		r.observeRedis("sliding_window_add", time.Since(startedAt))
	}()

	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	windowKey := slidingWindowKey(event.PatientID)
	dataKey := slidingWindowDataKey(event.PatientID)
	score := event.Timestamp.Unix()
	cutoff := event.Timestamp.Add(-SlidingWindowDuration).Unix()
	oldMax := exclusiveScore(cutoff)

	if err := r.client.ZAdd(ctx, windowKey, goredis.Z{
		Score:  float64(score),
		Member: event.EventID,
	}).Err(); err != nil {
		return nil, err
	}
	if err := r.client.HSet(ctx, dataKey, event.EventID, string(payload)).Err(); err != nil {
		return nil, err
	}

	oldEventIDs, err := r.client.ZRangeByScore(ctx, windowKey, &goredis.ZRangeBy{
		Min: "-inf",
		Max: oldMax,
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(oldEventIDs) > 0 {
		if err := r.client.ZRemRangeByScore(ctx, windowKey, "-inf", oldMax).Err(); err != nil {
			return nil, err
		}
		if err := r.client.HDel(ctx, dataKey, oldEventIDs...).Err(); err != nil {
			return nil, err
		}
	}

	eventIDs, err := r.client.ZRangeByScore(ctx, windowKey, &goredis.ZRangeBy{
		Min: strconv.FormatInt(cutoff, 10),
		Max: strconv.FormatInt(score, 10),
	}).Result()
	if err != nil {
		return nil, err
	}

	events, err := r.loadEvents(ctx, dataKey, eventIDs)
	if err != nil {
		return nil, err
	}

	if err := r.expireKeys(ctx, windowKey, dataKey); err != nil {
		return nil, err
	}

	return events, nil
}

func (r *SlidingWindowRepository) loadEvents(
	ctx context.Context,
	dataKey string,
	eventIDs []string,
) ([]model.TelemetryEvent, error) {
	if len(eventIDs) == 0 {
		return []model.TelemetryEvent{}, nil
	}

	values, err := r.client.HMGet(ctx, dataKey, eventIDs...).Result()
	if err != nil {
		return nil, err
	}

	events := make([]model.TelemetryEvent, 0, len(values))
	for i, value := range values {
		if value == nil {
			return nil, fmt.Errorf("missing telemetry payload for event_id %s", eventIDs[i])
		}

		payload, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected telemetry payload type %T", value)
		}

		var event model.TelemetryEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

func (r *SlidingWindowRepository) expireKeys(ctx context.Context, keys ...string) error {
	for _, key := range keys {
		if err := r.client.Expire(ctx, key, r.ttl).Err(); err != nil {
			return err
		}
	}

	return nil
}

func slidingWindowKey(patientID string) string {
	return fmt.Sprintf("processing:window:%s", patientID)
}

func slidingWindowDataKey(patientID string) string {
	return fmt.Sprintf("processing:window:%s:events", patientID)
}

func exclusiveScore(score int64) string {
	return fmt.Sprintf("(%d", score)
}

func (r *SlidingWindowRepository) observeRedis(operation string, duration time.Duration) {
	if r.metrics == nil {
		return
	}

	r.metrics.ObserveHistogram(
		"processing_redis_duration_seconds",
		metrics.Labels{"operation": operation},
		duration.Seconds(),
	)
}
