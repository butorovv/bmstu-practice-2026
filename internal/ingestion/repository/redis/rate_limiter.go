package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
	goredis "github.com/redis/go-redis/v9"
)

const RateLimitInterval = 5 * time.Second

const tokenBucketScript = `
local interval_ms = tonumber(ARGV[1])
local acquired = redis.call("SET", KEYS[1], "1", "NX", "PX", interval_ms)

if acquired then
	return {1, 0}
end

local ttl_ms = redis.call("PTTL", KEYS[1])
if ttl_ms < 1 then
	ttl_ms = interval_ms
end

return {0, math.ceil(ttl_ms / 1000)}
`

type rateLimitRedisClient interface {
	Eval(
		ctx context.Context,
		script string,
		keys []string,
		args ...interface{},
	) *goredis.Cmd
}

type RateLimiter struct {
	client rateLimitRedisClient
}

var _ usecase.RateLimiter = (*RateLimiter)(nil)

func NewRateLimiter(client rateLimitRedisClient) *RateLimiter {
	return &RateLimiter{client: client}
}

func (l *RateLimiter) Allow(
	ctx context.Context,
	deviceID string,
) (usecase.RateLimitDecision, error) {
	result, err := l.client.Eval(
		ctx,
		tokenBucketScript,
		[]string{rateLimitKey(deviceID)},
		RateLimitInterval.Milliseconds(),
	).Result()
	if err != nil {
		return usecase.RateLimitDecision{}, err
	}

	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return usecase.RateLimitDecision{}, errors.New("unexpected Redis rate limit response")
	}

	allowed, err := redisInteger(values[0])
	if err != nil {
		return usecase.RateLimitDecision{}, err
	}
	retryAfterSeconds, err := redisInteger(values[1])
	if err != nil {
		return usecase.RateLimitDecision{}, err
	}

	return usecase.RateLimitDecision{
		Allowed:    allowed == 1,
		RetryAfter: time.Duration(retryAfterSeconds) * time.Second,
	}, nil
}

func rateLimitKey(deviceID string) string {
	return fmt.Sprintf("rate:%s", deviceID)
}

func redisInteger(value interface{}) (int64, error) {
	switch number := value.(type) {
	case int64:
		return number, nil
	case int:
		return int64(number), nil
	default:
		return 0, fmt.Errorf("unexpected Redis integer type %T", value)
	}
}
