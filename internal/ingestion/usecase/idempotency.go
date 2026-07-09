package usecase

import "context"

type IdempotencyRepository interface {
	Reserve(ctx context.Context, deviceID string, batchID string) (bool, error)
	Release(ctx context.Context, deviceID string, batchID string) error
}
