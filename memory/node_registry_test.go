package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

type blockingStrategy struct {
	started chan struct{}
	release chan struct{}
}

func (s blockingStrategy) Choose(_ context.Context, input sp.StrategyInput) (sp.Node, error) {
	close(s.started)
	<-s.release
	return input.EffectiveNodes[0], nil
}

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

func TestNodeRegistryRenewNodeRejectsOfflineNode(t *testing.T) {
	ctx := context.Background()
	registry := NewNodeRegistry(NewEventBus())
	registry.SetHeartbeatTTL(time.Nanosecond)
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
	if err := registry.ExpireHeartbeats(ctx, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("ExpireHeartbeats error: %v", err)
	}

	if err := registry.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeNotFound) {
		t.Fatalf("RenewNode offline err = %v, want ErrNodeNotFound", err)
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

func TestCompleteDrainRejectsNodeWithPlacements(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	node := registerTestNode(t, dir, "game-1", "session-a")
	placement, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  node.NodeType,
		TargetNodeGroup: node.NodeGroup,
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	if err := dir.NodeRegistry().MarkNodeInvalid(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}
	if err := dir.NodeRegistry().DrainNode(ctx, node.NodeIdentity); err != nil {
		t.Fatalf("DrainNode error: %v", err)
	}

	if err := dir.NodeRegistry().CompleteDrain(ctx, node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeHasPlacements) {
		t.Fatalf("CompleteDrain with placement err = %v, want ErrNodeHasPlacements", err)
	}
	if err := dir.Release(ctx, sp.ReleaseCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
	}); err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if err := dir.NodeRegistry().CompleteDrain(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatalf("CompleteDrain after release error: %v", err)
	}
}

func TestCompleteDrainPreventsConcurrentAllocateCommit(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus()
	strategy := blockingStrategy{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	dir, err := NewDirectory(NewNodeRegistry(bus), sp.StrategyModeGo, strategy, bus)
	if err != nil {
		t.Fatal(err)
	}
	node := registerTestNode(t, dir, "game-1", "session-a")
	allocateDone := make(chan error, 1)
	go func() {
		_, err := dir.Allocate(ctx, sp.AllocateCommand{
			GrainID:         "10001",
			Kind:            "Player",
			TargetNodeType:  node.NodeType,
			TargetNodeGroup: node.NodeGroup,
			LeaseTTL:        time.Minute,
		})
		allocateDone <- err
	}()
	<-strategy.started

	if err := dir.NodeRegistry().MarkNodeInvalid(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}
	if err := dir.NodeRegistry().DrainNode(ctx, node.NodeIdentity); err != nil {
		t.Fatalf("DrainNode error: %v", err)
	}
	if err := dir.NodeRegistry().CompleteDrain(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatalf("CompleteDrain error: %v", err)
	}
	close(strategy.release)

	if err := <-allocateDone; !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("concurrent Allocate err = %v, want ErrNoAvailableNode", err)
	}
	key, err := sp.NewGrainKey("Player", "10001")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dir.Lookup(ctx, key); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Lookup after rejected Allocate err = %v, want ErrPlacementNotFound", err)
	}
}

func TestCompleteDrainValidatesSessionBeforePlacements(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	node := registerTestNode(t, dir, "game-1", "session-a")
	if _, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  node.NodeType,
		TargetNodeGroup: node.NodeGroup,
		LeaseTTL:        time.Minute,
	}); err != nil {
		t.Fatalf("Allocate error: %v", err)
	}

	if err := dir.NodeRegistry().CompleteDrain(ctx, node.NodeIdentity, "wrong-session"); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("CompleteDrain wrong session err = %v, want ErrInvalidNodeSession", err)
	}
	if err := dir.NodeRegistry().UnregisterNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatalf("UnregisterNode error: %v", err)
	}
	if err := dir.NodeRegistry().CompleteDrain(ctx, node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeNotFound) {
		t.Fatalf("CompleteDrain missing node err = %v, want ErrNodeNotFound", err)
	}
}
