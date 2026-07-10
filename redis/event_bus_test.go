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

func TestRedisEventBusRejectsMismatchedConsumerGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer := StreamConsumer{
		NodeIdentity:  "game/default/game-2",
		NodeSessionID: "session-b",
		Group:         ConsumerGroupName("game/default/game-1", "session-a"),
	}
	bus := NewEventBus(client, consumer)

	err := bus.EnsureConsumerGroup(ctx)
	if !errors.Is(err, ErrSharedConsumerGroup) {
		t.Fatalf("EnsureConsumerGroup err = %v, want ErrSharedConsumerGroup", err)
	}
	if !bus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}

func TestRedisEventBusSharedConsumerGroupWouldLoadBalanceAndIsRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	sharedGroup := ConsumerGroupName("game/default/game-1", "session-a")
	if err := client.XGroupCreateMkStream(ctx, EventsStreamKey(), sharedGroup, "$").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream error: %v", err)
	}
	bus := NewEventBus(client, StreamConsumer{
		NodeIdentity:  "game/default/game-1",
		NodeSessionID: "session-a",
		Group:         sharedGroup,
	})
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	first, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    sharedGroup,
		Consumer: "session-a",
		Streams:  []string{EventsStreamKey(), ">"},
		Count:    1,
		Block:    10 * time.Millisecond,
	}).Result()
	if err != nil {
		t.Fatalf("first XReadGroup error: %v", err)
	}
	if len(first) != 1 || len(first[0].Messages) != 1 {
		t.Fatalf("first consumer messages = %+v", first)
	}
	second, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    sharedGroup,
		Consumer: "session-b",
		Streams:  []string{EventsStreamKey(), ">"},
		Count:    1,
		Block:    10 * time.Millisecond,
	}).Result()
	if err != goredis.Nil {
		t.Fatalf("second XReadGroup err = %v, messages = %+v", err, second)
	}

	rejected := NewEventBus(client, StreamConsumer{
		NodeIdentity:  "game/default/game-2",
		NodeSessionID: "session-b",
		Group:         sharedGroup,
	})
	if err := rejected.EnsureConsumerGroup(ctx); !errors.Is(err, ErrSharedConsumerGroup) {
		t.Fatalf("EnsureConsumerGroup err = %v, want ErrSharedConsumerGroup", err)
	}
}

func TestRedisEventBusCleansOldSessionGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("old consumer error: %v", err)
	}
	newConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}
	oldBus := NewEventBus(client, oldConsumer)
	if err := oldBus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("old EnsureConsumerGroup error: %v", err)
	}
	newBus := NewEventBus(client, newConsumer)
	if err := newBus.CleanupConsumerGroup(ctx, oldConsumer); err != nil {
		t.Fatalf("CleanupConsumerGroup error: %v", err)
	}
	if err := newBus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("new EnsureConsumerGroup error: %v", err)
	}
	groups, err := client.XInfoGroups(ctx, EventsStreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups error: %v", err)
	}
	for _, group := range groups {
		if group.Name == oldConsumer.Group {
			t.Fatalf("old consumer group still exists: %+v", groups)
		}
	}
}

func TestRedisEventBusPendingTimeoutEntersDegraded(t *testing.T) {
	ctx := context.Background()
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
	handlerErr := errors.New("leave pending")
	if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error { return handlerErr }); !errors.Is(err, handlerErr) {
		t.Fatalf("Subscribe err = %v", err)
	}

	checkBus := NewEventBus(client, consumer)
	if err := checkBus.CheckPending(ctx, 0); err == nil {
		t.Fatal("CheckPending succeeded, want pending timeout error")
	}
	if !checkBus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}

func TestRedisEventBusTrimKeepsPendingMessages(t *testing.T) {
	ctx := context.Background()
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
		t.Fatalf("Publish first error: %v", err)
	}
	if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error { return errors.New("leave pending") }); err == nil {
		t.Fatal("Subscribe succeeded, want handler error")
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementTransferred, GrainKey: sp.GrainKey("Player/10002")}); err != nil {
		t.Fatalf("Publish second error: %v", err)
	}

	if err := bus.Trim(ctx, 1); err != nil {
		t.Fatalf("Trim error: %v", err)
	}
	messages, err := client.XRange(ctx, EventsStreamKey(), "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("stream len after safe trim = %d, want 2", len(messages))
	}
}

func TestRedisEventBusTrimGapEntersDegraded(t *testing.T) {
	ctx := context.Background()
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
		t.Fatalf("Publish first error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementTransferred, GrainKey: sp.GrainKey("Player/10002")}); err != nil {
		t.Fatalf("Publish second error: %v", err)
	}
	if err := client.XTrimMaxLen(ctx, EventsStreamKey(), 1).Err(); err != nil {
		t.Fatalf("force trim error: %v", err)
	}
	if err := bus.CheckContinuity(ctx); err == nil {
		t.Fatal("CheckContinuity succeeded, want trim gap error")
	}
	if !bus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}

func TestRedisEventBusRunTrimLoopTrimsPeriodically(t *testing.T) {
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
	for i := 0; i < 2; i++ {
		if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
	}
	trimCtx, trimCancel := context.WithCancel(ctx)
	defer trimCancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- bus.RunTrimLoop(trimCtx, 10*time.Millisecond, 1)
	}()

	for {
		length, err := client.XLen(ctx, EventsStreamKey()).Result()
		if err != nil {
			t.Fatalf("XLen error: %v", err)
		}
		if length == 1 {
			trimCancel()
			if err := <-errCh; err != nil {
				t.Fatalf("RunTrimLoop error: %v", err)
			}
			return
		}
		select {
		case err := <-errCh:
			t.Fatalf("RunTrimLoop returned early: %v", err)
		case <-ctx.Done():
			t.Fatalf("stream len after trim loop = %d, want 1", length)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRedisEventBusPubSubHintIsOptionalAndDoesNotWriteStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	got := make(chan sp.PlacementEvent, 1)
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- bus.SubscribeHint(subCtx, func(event sp.PlacementEvent) error {
			select {
			case got <- event:
			default:
			}
			subCancel()
			return nil
		})
	}()

	event := sp.PlacementEvent{Type: sp.EventPlacementTransferred, GrainKey: sp.GrainKey("Player/10001")}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case received := <-got:
			if received.Type != event.Type || received.GrainKey != event.GrainKey {
				t.Fatalf("hint event = %+v", received)
			}
			if err := <-errCh; err != nil {
				t.Fatalf("SubscribeHint error: %v", err)
			}
			length, err := client.XLen(ctx, EventsStreamKey()).Result()
			if err != nil {
				t.Fatalf("XLen error: %v", err)
			}
			if length != 0 {
				t.Fatalf("PublishHint wrote stream len = %d, want 0", length)
			}
			return
		case <-ticker.C:
			if err := bus.PublishHint(ctx, event); err != nil {
				t.Fatalf("PublishHint error: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("hint subscriber did not receive event")
		}
	}
}
