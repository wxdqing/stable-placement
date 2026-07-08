package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func TestRedisEventBusBroadcastsToUniqueConsumerGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})

	firstConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("first consumer error: %v", err)
	}
	secondConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-2", NodeSessionID: "session-b"})
	if err != nil {
		t.Fatalf("second consumer error: %v", err)
	}
	first := NewEventBus(client, firstConsumer)
	second := NewEventBus(client, secondConsumer)
	if err := first.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("first EnsureConsumerGroup error: %v", err)
	}
	if err := second.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("second EnsureConsumerGroup error: %v", err)
	}
	event := sp.PlacementEvent{
		Type:             sp.EventPlacementTransferred,
		GrainKey:         sp.GrainKey("Player/10001"),
		NodeIdentity:     "game/default/game-2",
		PlacementVersion: 2,
		LeaseVersion:     1,
	}
	if err := first.Publish(ctx, event); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	gotFirst := make(chan sp.PlacementEvent, 1)
	gotSecond := make(chan sp.PlacementEvent, 1)
	firstCtx, firstCancel := context.WithCancel(ctx)
	secondCtx, secondCancel := context.WithCancel(ctx)
	defer firstCancel()
	defer secondCancel()
	go func() {
		_ = first.Subscribe(firstCtx, func(event sp.PlacementEvent) error {
			gotFirst <- event
			firstCancel()
			return nil
		})
	}()
	go func() {
		_ = second.Subscribe(secondCtx, func(event sp.PlacementEvent) error {
			gotSecond <- event
			secondCancel()
			return nil
		})
	}()

	for name, ch := range map[string]chan sp.PlacementEvent{"first": gotFirst, "second": gotSecond} {
		select {
		case got := <-ch:
			if got.Type != event.Type || got.GrainKey != event.GrainKey {
				t.Fatalf("%s event = %+v", name, got)
			}
		case <-ctx.Done():
			t.Fatalf("%s subscriber did not receive event", name)
		}
	}
}

func TestRedisEventBusHandlerErrorEntersDegraded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	handlerErr := errors.New("handler failed")
	err = bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		return handlerErr
	})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("Subscribe err = %v", err)
	}
	if !bus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}

func TestRedisEventBusRetriesPendingMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementReleased, GrainKey: sp.GrainKey("Player/10001")}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	handlerErr := errors.New("first handler failed")
	if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		return handlerErr
	}); !errors.Is(err, handlerErr) {
		t.Fatalf("first Subscribe err = %v", err)
	}

	retryBus := NewEventBus(client, consumer)
	got := make(chan sp.PlacementEvent, 1)
	retryCtx, retryCancel := context.WithCancel(ctx)
	defer retryCancel()
	go func() {
		_ = retryBus.Subscribe(retryCtx, func(event sp.PlacementEvent) error {
			got <- event
			retryCancel()
			return nil
		})
	}()
	select {
	case event := <-got:
		if event.Type != sp.EventPlacementReleased {
			t.Fatalf("retried event = %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("pending event was not retried")
	}
}

func TestRedisEventBusMalformedEventEntersDegraded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: EventsStreamKey(),
		Values: map[string]any{"grain_key": "Player/10001"},
	}).Err(); err != nil {
		t.Fatalf("XAdd malformed error: %v", err)
	}

	err = bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		t.Fatal("handler should not be called for malformed event")
		return nil
	})
	if err == nil {
		t.Fatal("Subscribe succeeded for malformed event")
	}
	if !bus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}
