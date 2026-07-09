package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type fakeRedisClient struct {
	setNXResult bool
	setNXErr    error
	delErr      error
	key         string
	value       interface{}
	expiration  time.Duration
	deletedKey  string
}

func (f *fakeRedisClient) SetNX(
	_ context.Context,
	key string,
	value interface{},
	expiration time.Duration,
) *goredis.BoolCmd {
	f.key = key
	f.value = value
	f.expiration = expiration
	return goredis.NewBoolResult(f.setNXResult, f.setNXErr)
}

func (f *fakeRedisClient) Del(_ context.Context, keys ...string) *goredis.IntCmd {
	if len(keys) > 0 {
		f.deletedKey = keys[0]
	}
	return goredis.NewIntResult(1, f.delErr)
}

func TestIdempotencyRepositoryReserveUsesContractKeyAndTTL(t *testing.T) {
	client := &fakeRedisClient{setNXResult: true}
	repository := NewIdempotencyRepository(client)

	reserved, err := repository.Reserve(context.Background(), "device-001", "device-001-000001")

	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if !reserved {
		t.Fatal("Reserve() = false, want true")
	}
	if client.key != "idempotency:device-001:device-001-000001" {
		t.Fatalf("key = %q", client.key)
	}
	if client.expiration != 24*time.Hour {
		t.Fatalf("expiration = %v, want 24h", client.expiration)
	}
}

func TestIdempotencyRepositoryReserveReturnsDuplicate(t *testing.T) {
	repository := NewIdempotencyRepository(&fakeRedisClient{setNXResult: false})

	reserved, err := repository.Reserve(context.Background(), "device-001", "batch-001")

	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if reserved {
		t.Fatal("Reserve() = true, want false")
	}
}

func TestIdempotencyRepositoryReserveReturnsRedisError(t *testing.T) {
	wantErr := errors.New("Redis unavailable")
	repository := NewIdempotencyRepository(&fakeRedisClient{setNXErr: wantErr})

	_, err := repository.Reserve(context.Background(), "device-001", "batch-001")

	if !errors.Is(err, wantErr) {
		t.Fatalf("Reserve() error = %v, want %v", err, wantErr)
	}
}

func TestIdempotencyRepositoryReleaseDeletesContractKey(t *testing.T) {
	client := &fakeRedisClient{}
	repository := NewIdempotencyRepository(client)

	if err := repository.Release(context.Background(), "device-001", "batch-001"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if client.deletedKey != "idempotency:device-001:batch-001" {
		t.Fatalf("deleted key = %q", client.deletedKey)
	}
}
