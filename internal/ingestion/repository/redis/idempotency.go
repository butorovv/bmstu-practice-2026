package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
	goredis "github.com/redis/go-redis/v9"
)

const IdempotencyTTL = 24 * time.Hour

type redisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.BoolCmd
	Del(ctx context.Context, keys ...string) *goredis.IntCmd
	Close() error
}

type IdempotencyRepository struct {
	client redisClient
}

var _ usecase.IdempotencyRepository = (*IdempotencyRepository)(nil)

func NewIdempotencyRepository(addr string, password string, db int) *IdempotencyRepository {
	client := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return newIdempotencyRepository(client)
}

func newIdempotencyRepository(client redisClient) *IdempotencyRepository {
	return &IdempotencyRepository{client: client}
}

func (r *IdempotencyRepository) Reserve(
	ctx context.Context,
	deviceID string,
	batchID string,
) (bool, error) {
	return r.client.SetNX(ctx, idempotencyKey(deviceID, batchID), "1", IdempotencyTTL).Result()
}

func (r *IdempotencyRepository) Release(
	ctx context.Context,
	deviceID string,
	batchID string,
) error {
	return r.client.Del(ctx, idempotencyKey(deviceID, batchID)).Err()
}

func (r *IdempotencyRepository) Close() error {
	return r.client.Close()
}

func idempotencyKey(deviceID string, batchID string) string {
	return fmt.Sprintf("idempotency:%s:%s", deviceID, batchID)
}
