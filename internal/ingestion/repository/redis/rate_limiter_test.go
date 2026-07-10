package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type fakeRateLimitRedisClient struct {
	result interface{}
	err    error
	script string
	keys   []string
	args   []interface{}
}

func (f *fakeRateLimitRedisClient) Eval(
	_ context.Context,
	script string,
	keys []string,
	args ...interface{},
) *goredis.Cmd {
	f.script = script
	f.keys = keys
	f.args = args
	return goredis.NewCmdResult(f.result, f.err)
}

func TestRateLimiterAllowsBatchUsingContractKey(t *testing.T) {
	client := &fakeRateLimitRedisClient{result: []interface{}{int64(1), int64(0)}}
	limiter := NewRateLimiter(client)

	decision, err := limiter.Allow(context.Background(), "device-001")

	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	if !decision.Allowed {
		t.Fatal("Allowed = false, want true")
	}
	if decision.RetryAfter != 0 {
		t.Fatalf("RetryAfter = %v, want 0", decision.RetryAfter)
	}
	if len(client.keys) != 1 || client.keys[0] != "rate:device-001" {
		t.Fatalf("keys = %v, want [rate:device-001]", client.keys)
	}
	if len(client.args) != 1 || client.args[0] != int64(5000) {
		t.Fatalf("args = %v, want [5000]", client.args)
	}
}

func TestRateLimiterRejectsBatchWithRetryAfter(t *testing.T) {
	client := &fakeRateLimitRedisClient{result: []interface{}{int64(0), int64(4)}}
	limiter := NewRateLimiter(client)

	decision, err := limiter.Allow(context.Background(), "device-001")

	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	if decision.Allowed {
		t.Fatal("Allowed = true, want false")
	}
	if decision.RetryAfter != 4*time.Second {
		t.Fatalf("RetryAfter = %v, want 4s", decision.RetryAfter)
	}
}

func TestRateLimiterReturnsRedisError(t *testing.T) {
	wantErr := errors.New("Redis unavailable")
	limiter := NewRateLimiter(&fakeRateLimitRedisClient{err: wantErr})

	_, err := limiter.Allow(context.Background(), "device-001")

	if !errors.Is(err, wantErr) {
		t.Fatalf("Allow() error = %v, want %v", err, wantErr)
	}
}
