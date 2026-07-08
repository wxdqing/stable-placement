package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

func TestNodeRegistryInvalidGroupSurvivesSessionReplacement(t *testing.T) {
	ctx := context.Background()
	registry := NewNodeRegistry(NewEventBus())
	node := sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      "game-1",
		NodeIdentity:  "game/default/game-1",
		NodeSessionID: "session-a",
		Status:        sp.NodeStatusActive,
	}
	if err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	if err := registry.MarkNodeInvalid(ctx, "game", "default", "game-1"); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}
	node.NodeSessionID = "session-b"
	if _, err := registry.ReplaceNodeSession(ctx, node); err != nil {
		t.Fatalf("ReplaceNodeSession error: %v", err)
	}
	if !registry.IsInvalid("game", "default", "game-1") {
		t.Fatal("invalid node group did not survive session replacement")
	}
	if err := registry.RenewNode(ctx, node.NodeIdentity, "session-a"); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("old session renew err = %v, want ErrInvalidNodeSession", err)
	}
}

func TestNodeRegistryPublishesMarkAndRestoreEvents(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus()
	registry := NewNodeRegistry(bus)
	var seen []sp.EventType
	_ = bus.Subscribe(ctx, func(event sp.PlacementEvent) error {
		seen = append(seen, event.Type)
		return nil
	})

	if err := registry.MarkNodeInvalid(ctx, "game", "default", "game-1"); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}
	if err := registry.RestoreNode(ctx, "game", "default", "game-1"); err != nil {
		t.Fatalf("RestoreNode error: %v", err)
	}

	if len(seen) != 2 || seen[0] != sp.EventNodeMarkedInvalid || seen[1] != sp.EventNodeRestored {
		t.Fatalf("events = %+v", seen)
	}
}

func TestNodeRegistryDrainRequiresInvalidNodeAndHeartbeatTimeout(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus()
	registry := NewNodeRegistry(bus)
	node := sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      "game-1",
		NodeIdentity:  "game/default/game-1",
		NodeSessionID: "session-a",
		Status:        sp.NodeStatusActive,
	}
	if err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	if err := registry.DrainNode(ctx, node.NodeIdentity); !errors.Is(err, sp.ErrNodeNotInvalid) {
		t.Fatalf("DrainNode before invalid err = %v, want ErrNodeNotInvalid", err)
	}
	if err := registry.MarkNodeInvalid(ctx, "game", "default", "game-1"); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}
	if err := registry.DrainNode(ctx, node.NodeIdentity); err != nil {
		t.Fatalf("DrainNode error: %v", err)
	}
	draining, ok := registry.Node(node.NodeIdentity)
	if !ok || draining.Status != sp.NodeStatusDraining {
		t.Fatalf("node after drain = %+v, ok=%v", draining, ok)
	}

	registry.SetHeartbeatTTL(time.Nanosecond)
	time.Sleep(time.Millisecond)
	if err := registry.ExpireHeartbeats(ctx, time.Now()); err != nil {
		t.Fatalf("ExpireHeartbeats error: %v", err)
	}
	offline, ok := registry.Node(node.NodeIdentity)
	if !ok || offline.Status != sp.NodeStatusOffline {
		t.Fatalf("node after heartbeat timeout = %+v, ok=%v", offline, ok)
	}
}
