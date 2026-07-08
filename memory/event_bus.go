package memory

import (
	"context"
	"sync"

	sp "github.com/wxdqing/stable-placement"
)

type EventBus struct {
	mu       sync.RWMutex
	handlers []func(sp.PlacementEvent) error
}

func NewEventBus() *EventBus {
	return &EventBus{}
}

func (b *EventBus) Subscribe(_ context.Context, handler func(sp.PlacementEvent) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, handler)
	return nil
}

func (b *EventBus) Publish(ctx context.Context, event sp.PlacementEvent) error {
	b.mu.RLock()
	handlers := append([]func(sp.PlacementEvent) error(nil), b.handlers...)
	b.mu.RUnlock()

	for _, handler := range handlers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handler(event); err != nil {
			return err
		}
	}
	return nil
}
