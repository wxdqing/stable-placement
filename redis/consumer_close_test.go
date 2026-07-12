package redis

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func TestCloseConsumerGroupIfIdlePreservesPendingGroup(t *testing.T) {
	ctx := context.Background()
	bus, client, consumer := newConsumerCloseTestBus(t)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementCreated, GrainKey: "player/pending"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: consumer.Group, Consumer: consumer.NodeSessionID,
		Streams: []string{bus.StreamKey(), ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatal(err)
	}

	if err := bus.CloseConsumerGroupIfIdle(ctx); !errors.Is(err, ErrPendingMessages) {
		t.Fatalf("CloseConsumerGroupIfIdle error = %v", err)
	}
	requireConsumerGroup(t, client, bus.StreamKey(), consumer.Group, true)
}

func TestCloseConsumerGroupIfIdlePreservesLaggingGroup(t *testing.T) {
	ctx := context.Background()
	bus, client, consumer := newConsumerCloseTestBus(t)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementCreated, GrainKey: "player/lag"}); err != nil {
		t.Fatal(err)
	}

	if err := bus.CloseConsumerGroupIfIdle(ctx); !errors.Is(err, ErrPendingMessages) {
		t.Fatalf("CloseConsumerGroupIfIdle error = %v", err)
	}
	requireConsumerGroup(t, client, bus.StreamKey(), consumer.Group, true)
}

func TestCloseConsumerGroupIfIdleDeletesOnlyIdleGroup(t *testing.T) {
	ctx := context.Background()
	bus, client, consumer := newConsumerCloseTestBus(t)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bus.CloseConsumerGroupIfIdle(ctx); err != nil {
		t.Fatal(err)
	}
	requireConsumerGroup(t, client, bus.StreamKey(), consumer.Group, false)
	if err := bus.CloseConsumerGroupIfIdle(ctx); err != nil {
		t.Fatalf("missing group close: %v", err)
	}
}

func newConsumerCloseTestBus(t *testing.T) (*EventBus, *goredis.Client, StreamConsumer) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-close", NodeSessionID: "session-close"})
	if err != nil {
		t.Fatal(err)
	}
	return NewEventBus(client, consumer), client, consumer
}

func requireConsumerGroup(t *testing.T, client *goredis.Client, stream, group string, want bool) {
	t.Helper()
	groups, err := client.XInfoGroups(context.Background(), stream).Result()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range groups {
		if item.Name == group {
			found = true
		}
	}
	if found != want {
		t.Fatalf("group %q found=%v, want %v", group, found, want)
	}
}
