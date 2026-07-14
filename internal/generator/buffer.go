package generator

import (
	"context"
	"sync"
)

type measurementBuffer struct {
	mu     sync.Mutex
	items  []Measurement
	closed bool
	notify chan struct{}
}

func newMeasurementBuffer() *measurementBuffer {
	return &measurementBuffer{notify: make(chan struct{}, 1)}
}

func (b *measurementBuffer) Push(measurement Measurement) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	b.items = append(b.items, measurement)
	b.signal()
	return true
}

func (b *measurementBuffer) Snapshot(preferred int, maximum int) ([]Measurement, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.items) == 0 {
		return nil, b.closed
	}
	if len(b.items) < preferred && !b.closed {
		return nil, false
	}

	count := preferred
	if len(b.items) > preferred || b.closed {
		count = min(len(b.items), maximum)
	}
	measurements := make([]Measurement, count)
	copy(measurements, b.items[:count])
	return measurements, false
}

func (b *measurementBuffer) Remove(count int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if count >= len(b.items) {
		b.items = nil
		return
	}
	copy(b.items, b.items[count:])
	b.items = b.items[:len(b.items)-count]
}

func (b *measurementBuffer) Close() {
	b.mu.Lock()
	b.closed = true
	b.signal()
	b.mu.Unlock()
}

func (b *measurementBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

func (b *measurementBuffer) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.notify:
		return nil
	}
}

func (b *measurementBuffer) signal() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}
