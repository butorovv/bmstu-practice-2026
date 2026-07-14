package generator

import (
	"context"
	"sync"
	"time"
)

type requestPacer struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newRequestPacer(rps float64) *requestPacer {
	interval := time.Duration(float64(time.Second) / rps)
	if interval < time.Nanosecond {
		interval = time.Nanosecond
	}
	return &requestPacer{interval: interval}
}

func (p *requestPacer) Wait(ctx context.Context) error {
	p.mu.Lock()
	now := time.Now()
	if p.next.Before(now) {
		p.next = now
	}
	scheduled := p.next
	p.next = p.next.Add(p.interval)
	p.mu.Unlock()

	delay := time.Until(scheduled)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
