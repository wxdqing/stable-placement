package stableplacement_test

import (
	"context"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/memory"
	"github.com/wxdqing/stable-placement/strategies"
)

func TestStablePlacementFirstPhaseFlow(t *testing.T) {
	ctx := context.Background()
	bus := memory.NewEventBus()
	dir, err := memory.NewDirectory(memory.NewNodeRegistry(bus), sp.StrategyModeGo, strategies.NewRoundRobin(), bus)
	if err != nil {
		t.Fatalf("NewDirectory error: %v", err)
	}
	cache := memory.NewPlacementCache()

	_ = bus.Subscribe(ctx, func(event sp.PlacementEvent) error {
		switch {
		case event.GrainKey.String() != "":
			cache.DeleteCachedPlacement(event.GrainKey)
		case event.NodeIdentity != "":
			cache.DeleteCachedPlacementsByNode(event.NodeIdentity)
		default:
			cache.ClearPlacementCache()
		}
		return nil
	})

	nodeID, _ := sp.NewNodeIdentity("game", "default", "game-1")
	node := sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      "game-1",
		NodeIdentity:  nodeID.String(),
		NodeSessionID: "session-a",
		Status:        sp.NodeStatusActive,
	}
	if err := dir.NodeRegistry().RegisterNode(ctx, node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}

	placement, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	cache.SetCachedPlacement(placement.GrainKey, sp.PlacementRoute{GrainKey: placement.GrainKey, NodeIdentity: placement.NodeIdentity})
	if _, ok := cache.GetCachedPlacement(placement.GrainKey); !ok {
		t.Fatal("cache did not store placement")
	}

	if err := dir.NodeRegistry().MarkNodeInvalid(ctx, "game", "default", "game-1"); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}
	if _, ok := cache.GetCachedPlacement(placement.GrainKey); ok {
		t.Fatal("cache did not clear after node invalid event")
	}

	cache.SetDegraded(true)
	cache.SetCachedPlacement(placement.GrainKey, sp.PlacementRoute{GrainKey: placement.GrainKey, NodeIdentity: placement.NodeIdentity})
	if _, ok := cache.GetCachedPlacement(placement.GrainKey); ok {
		t.Fatal("degraded cache returned stale route")
	}
}
