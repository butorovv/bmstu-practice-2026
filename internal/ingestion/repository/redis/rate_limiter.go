package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
	goredis "github.com/redis/go-redis/v9"
)

const (
	RateLimitInterval = 5 * time.Second
	RateLimitBurst    = 2
)

const tokenBucketScript = `
local interval_ms = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])

local key_type = redis.call("TYPE", KEYS[1])
if type(key_type) == "table" then
	key_type = key_type["ok"]
end
if key_type ~= "none" and key_type ~= "hash" then
	redis.call("DEL", KEYS[1])
end

local redis_time = redis.call("TIME")
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local state = redis.call("HMGET", KEYS[1], "tokens", "refilled_at")
local tokens = tonumber(state[1])
local refilled_at = tonumber(state[2])

if not tokens or not refilled_at then
	tokens = capacity
	refilled_at = now_ms
else
	local elapsed_ms = math.max(0, now_ms - refilled_at)
	tokens = math.min(capacity, tokens + elapsed_ms / interval_ms)
	refilled_at = now_ms
end

local allowed = 0
local retry_after_ms = 0
if tokens >= 1 then
	tokens = tokens - 1
	allowed = 1
else
	retry_after_ms = math.ceil((1 - tokens) * interval_ms)
end

redis.call("HSET", KEYS[1], "tokens", tokens, "refilled_at", refilled_at)
redis.call("PEXPIRE", KEYS[1], ttl_ms)

return {allowed, retry_after_ms}
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
	burst  int
}

var _ usecase.RateLimiter = (*RateLimiter)(nil)

func NewRateLimiter(client rateLimitRedisClient) *RateLimiter {
	return NewRateLimiterWithBurst(client, RateLimitBurst)
}

func NewRateLimiterWithBurst(client rateLimitRedisClient, burst int) *RateLimiter {
	if burst <= 0 {
		burst = RateLimitBurst
	}

	return &RateLimiter{client: client, burst: burst}
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
		l.burst,
		(RateLimitInterval * time.Duration(l.burst)).Milliseconds(),
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
	retryAfterMilliseconds, err := redisInteger(values[1])
	if err != nil {
		return usecase.RateLimitDecision{}, err
	}

	return usecase.RateLimitDecision{
		Allowed:    allowed == 1,
		RetryAfter: time.Duration(retryAfterMilliseconds) * time.Millisecond,
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
