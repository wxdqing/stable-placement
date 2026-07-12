package stableplacement_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/memory"
	spredis "github.com/wxdqing/stable-placement/redis"
	"github.com/wxdqing/stable-placement/strategies"
)

func TestStablePlacementFirstPhaseFlow(t *testing.T) {
	ctx := context.Background()
	bus := memory.NewEventBus()
	registry, err := memory.NewNodeRegistry(bus, sp.DefaultNodeLeaseConfig())
	if err != nil {
		t.Fatalf("NewNodeRegistry error: %v", err)
	}
	dir, err := memory.NewDirectory(registry, sp.StrategyModeGo, strategies.NewRoundRobin(), bus)
	if err != nil {
		t.Fatalf("NewDirectory error: %v", err)
	}
	cache := memory.NewPlacementCache()
	router := memory.NewCachedRouter(dir, cache)

	if err := bus.Subscribe(ctx, router.HandleEvent); err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	nodeID, _ := sp.NewNodeIdentity("game", "default", "game-1")
	node := sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      "game-1",
		NodeIdentity:  nodeID.String(),
		NodeSessionID: "session-a",
		Status:        sp.NodeStatusActive,
	}
	if _, err := dir.NodeRegistry().RegisterNode(ctx, node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}

	placement, err := router.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	if route, err := router.Lookup(ctx, placement.GrainKey); err != nil || route.NodeIdentity != placement.NodeIdentity {
		t.Fatalf("cached Lookup route = %+v, err = %v", route, err)
	}

	if err := dir.NodeRegistry().MarkNodeInvalid(ctx, "game", "default", "game-1"); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}
	if _, ok := cache.GetCachedPlacement(placement.GrainKey); ok {
		t.Fatal("cache did not clear after node invalid event")
	}

	router.Degrade()
	cache.SetCachedPlacement(placement.GrainKey, sp.PlacementRoute{GrainKey: placement.GrainKey, NodeIdentity: placement.NodeIdentity})
	if _, ok := cache.GetCachedPlacement(placement.GrainKey); ok {
		t.Fatal("degraded cache returned stale route")
	}
}

func TestRedisEventConsumerControlsCachedRouterHealth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	consumer, err := spredis.NewStreamConsumer(sp.Node{
		NodeIdentity:  "game/default/game-1",
		NodeSessionID: "session-a",
	})
	if err != nil {
		t.Fatalf("NewStreamConsumer error: %v", err)
	}
	eventBus := spredis.NewEventBus(client, consumer)
	registry, err := memory.NewNodeRegistry(nil, sp.DefaultNodeLeaseConfig())
	if err != nil {
		t.Fatalf("NewNodeRegistry error: %v", err)
	}
	directory, err := memory.NewDirectory(
		registry,
		sp.StrategyModeGo,
		strategies.NewRoundRobin(),
		nil,
	)
	if err != nil {
		t.Fatalf("NewDirectory error: %v", err)
	}
	cache := memory.NewPlacementCache()
	router := memory.NewCachedRouter(directory, cache)
	router.Degrade()

	if err := eventBus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := eventBus.CheckContinuity(ctx); err != nil {
		t.Fatalf("CheckContinuity error: %v", err)
	}
	router.Recover()
	if cache.IsDegraded() {
		t.Fatal("cache remained degraded after healthy consumer startup")
	}

	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: eventBus.StreamKey(),
		Values: map[string]any{"grain_key": "Player/10001"},
	}).Err(); err != nil {
		t.Fatalf("XAdd malformed event error: %v", err)
	}
	if err := eventBus.Subscribe(ctx, router.HandleEvent); err == nil {
		t.Fatal("Subscribe succeeded for malformed event")
	} else {
		router.Degrade()
	}
	if !cache.IsDegraded() {
		t.Fatal("cache did not degrade after Subscribe error")
	}
}
