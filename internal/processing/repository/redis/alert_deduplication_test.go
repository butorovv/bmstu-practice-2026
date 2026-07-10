package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	goredis "github.com/redis/go-redis/v9"
)

type fakeAlertDedupRedisClient struct {
	setNXResult bool
	setNXErr    error
	delErr      error
	key         string
	value       interface{}
	expiration  time.Duration
	deletedKey  string
}

func (f *fakeAlertDedupRedisClient) SetNX(
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

func (f *fakeAlertDedupRedisClient) Del(_ context.Context, keys ...string) *goredis.IntCmd {
	if len(keys) > 0 {
		f.deletedKey = keys[0]
	}
	return goredis.NewIntResult(1, f.delErr)
}

func TestAlertDeduplicatorReserveUsesSetNXWithContractKeyAndTTL(t *testing.T) {
	client := &fakeAlertDedupRedisClient{setNXResult: true}
	deduplicator := NewAlertDeduplicator(client, 5*time.Minute)

	reserved, err := deduplicator.Reserve(context.Background(), "patient-001", model.AlertTypeHighHeartRate)

	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if !reserved {
		t.Fatal("Reserve() = false, want true")
	}
	if client.key != "processing:alert:dedup:patient-001:HIGH_HEART_RATE" {
		t.Fatalf("key = %q", client.key)
	}
	if client.value != "1" {
		t.Fatalf("value = %v, want 1", client.value)
	}
	if client.expiration != 5*time.Minute {
		t.Fatalf("expiration = %v, want 5m", client.expiration)
	}
}

func TestAlertDeduplicatorReserveRejectsDuplicate(t *testing.T) {
	deduplicator := NewAlertDeduplicator(&fakeAlertDedupRedisClient{setNXResult: false}, 5*time.Minute)

	reserved, err := deduplicator.Reserve(context.Background(), "patient-001", model.AlertTypeHighHeartRate)

	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if reserved {
		t.Fatal("Reserve() = true, want false")
	}
}

func TestAlertDeduplicatorReserveReturnsRedisError(t *testing.T) {
	wantErr := errors.New("redis unavailable")
	deduplicator := NewAlertDeduplicator(&fakeAlertDedupRedisClient{setNXErr: wantErr}, 5*time.Minute)

	_, err := deduplicator.Reserve(context.Background(), "patient-001", model.AlertTypeHighHeartRate)

	if !errors.Is(err, wantErr) {
		t.Fatalf("Reserve() error = %v, want %v", err, wantErr)
	}
}

func TestAlertDeduplicatorReleaseDeletesContractKey(t *testing.T) {
	client := &fakeAlertDedupRedisClient{}
	deduplicator := NewAlertDeduplicator(client, 5*time.Minute)

	if err := deduplicator.Release(context.Background(), "patient-001", model.AlertTypeHighHeartRate); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if client.deletedKey != "processing:alert:dedup:patient-001:HIGH_HEART_RATE" {
		t.Fatalf("deleted key = %q", client.deletedKey)
	}
}
