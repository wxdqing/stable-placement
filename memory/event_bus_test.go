package memory

import (
	"context"
	"errors"
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

func TestEventBusPublishesToSubscribers(t *testing.T) {
	bus := NewEventBus()
	seen := make(chan sp.EventType, 1)

	if err := bus.Subscribe(context.Background(), func(event sp.PlacementEvent) error {
		seen <- event.Type
		return nil
	}); err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	if err := bus.Publish(context.Background(), sp.PlacementEvent{Type: sp.EventNodeMarkedInvalid}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	if got := <-seen; got != sp.EventNodeMarkedInvalid {
		t.Fatalf("event = %s", got)
	}
}

func TestEventBusReturnsHandlerError(t *testing.T) {
	bus := NewEventBus()
	want := errors.New("handler failed")
	_ = bus.Subscribe(context.Background(), func(event sp.PlacementEvent) error {
		return want
	})

	err := bus.Publish(context.Background(), sp.PlacementEvent{Type: sp.EventManualCacheClear})
	if !errors.Is(err, want) {
		t.Fatalf("Publish err = %v, want %v", err, want)
	}
}
