package usecase

import (
	"context"
	"time"
)

type RateLimitDecision struct {
	Allowed    bool
	RetryAfter time.Duration
}

type RateLimiter interface {
	Allow(ctx context.Context, deviceID string) (RateLimitDecision, error)
}
