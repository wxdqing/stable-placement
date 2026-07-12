package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/strategies"
)

func newTestDirectory(t *testing.T) (*Directory, *EventBus) {
	t.Helper()
	bus := NewEventBus()
	dir, err := NewDirectory(NewNodeRegistry(bus), sp.StrategyModeGo, strategies.NewRoundRobin(), bus)
	if err != nil {
		t.Fatal(err)
	}
	return dir, bus
}

func registerTestNode(t *testing.T, dir *Directory, name string, session string) sp.Node {
	t.Helper()
	id, err := sp.NewNodeIdentity("game", "default", name)
	if err != nil {
		t.Fatal(err)
	}
	node := sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      name,
		NodeIdentity:  id.String(),
		NodeSessionID: session,
		Status:        sp.NodeStatusActive,
	}
	if err := dir.NodeRegistry().RegisterNode(context.Background(), node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	return node
}

func TestDirectoryLookupRejectsExpiredLease(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	registerTestNode(t, dir, "game-1", "session-a")

	placement, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "expired-lookup",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	if _, err := dir.Lookup(ctx, placement.GrainKey); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Lookup expired lease err = %v, want ErrPlacementNotFound", err)
	}
}

func TestDirectoryAllocateLookupRenewTransferRelease(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	node1 := registerTestNode(t, dir, "game-1", "session-a")
	node2 := registerTestNode(t, dir, "game-2", "session-b")

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
	if placement.NodeIdentity != node1.NodeIdentity {
		t.Fatalf("allocated node = %q, want %q", placement.NodeIdentity, node1.NodeIdentity)
	}

	found, err := dir.Lookup(ctx, placement.GrainKey)
	if err != nil {
		t.Fatalf("Lookup error: %v", err)
	}
	if found.NodeIdentity != placement.NodeIdentity {
		t.Fatalf("lookup node = %q", found.NodeIdentity)
	}

	renewed, err := dir.Renew(ctx, sp.RenewCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node1.NodeIdentity,
		NodeSessionID:    node1.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
		ExtendTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Renew error: %v", err)
	}
	if renewed.Lease.Version != placement.Lease.Version+1 {
		t.Fatalf("lease version = %d", renewed.Lease.Version)
	}

	transferred, err := dir.Transfer(ctx, sp.TransferCommand{
		GrainKey:         placement.GrainKey,
		FromNodeIdentity: node1.NodeIdentity,
		ToNodeIdentity:   node2.NodeIdentity,
		PlacementVersion: renewed.Version,
		LeaseTTL:         time.Minute,
	})
	if err != nil {
		t.Fatalf("Transfer error: %v", err)
	}
	if transferred.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("transferred node = %q", transferred.NodeIdentity)
	}

	_, err = dir.Renew(ctx, sp.RenewCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node1.NodeIdentity,
		NodeSessionID:    node1.NodeSessionID,
		PlacementVersion: transferred.Version,
		LeaseVersion:     transferred.Lease.Version,
		ExtendTTL:        time.Minute,
	})
	if !errors.Is(err, sp.ErrInvalidOwner) {
		t.Fatalf("old owner renew err = %v, want ErrInvalidOwner", err)
	}

	if err := dir.Release(ctx, sp.ReleaseCommand{
		GrainKey:         transferred.GrainKey,
		NodeIdentity:     node2.NodeIdentity,
		NodeSessionID:    node2.NodeSessionID,
		PlacementVersion: transferred.Version,
		LeaseVersion:     transferred.Lease.Version,
	}); err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if _, err := dir.Lookup(ctx, transferred.GrainKey); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Lookup after release err = %v", err)
	}

	reallocated, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate after release error: %v", err)
	}
	if reallocated.Status != sp.PlacementStatusActive {
		t.Fatalf("reallocated status = %s", reallocated.Status)
	}
}

func TestDirectoryOldCommandCannotMutateReallocatedPlacement(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	registerTestNode(t, dir, "game-1", "session-a")

	first, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("first Allocate error: %v", err)
	}
	if err := dir.Release(ctx, sp.ReleaseCommand{
		GrainKey:         first.GrainKey,
		NodeIdentity:     first.NodeIdentity,
		NodeSessionID:    first.Lease.OwnerNodeSessionID,
		PlacementVersion: first.Version,
		LeaseVersion:     first.Lease.Version,
	}); err != nil {
		t.Fatalf("first Release error: %v", err)
	}

	second, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         first.GrainID,
		Kind:            first.Kind,
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("second Allocate error: %v", err)
	}
	if second.Version <= first.Version {
		t.Fatalf("reallocated version = %d, first = %d", second.Version, first.Version)
	}

	_, err = dir.Renew(ctx, sp.RenewCommand{
		GrainKey:         first.GrainKey,
		NodeIdentity:     first.NodeIdentity,
		NodeSessionID:    first.Lease.OwnerNodeSessionID,
		PlacementVersion: first.Version,
		LeaseVersion:     first.Lease.Version,
		ExtendTTL:        time.Minute,
	})
	if !errors.Is(err, sp.ErrVersionConflict) {
		t.Fatalf("old Renew err = %v", err)
	}
	err = dir.Release(ctx, sp.ReleaseCommand{
		GrainKey:         first.GrainKey,
		NodeIdentity:     first.NodeIdentity,
		NodeSessionID:    first.Lease.OwnerNodeSessionID,
		PlacementVersion: first.Version,
		LeaseVersion:     first.Lease.Version,
	})
	if !errors.Is(err, sp.ErrVersionConflict) {
		t.Fatalf("old Release err = %v", err)
	}
	active, err := dir.Lookup(ctx, second.GrainKey)
	if err != nil {
		t.Fatalf("Lookup second error: %v", err)
	}
	if active.Version != second.Version || active.Status != sp.PlacementStatusActive {
		t.Fatalf("active placement = %+v, want second = %+v", active, second)
	}
}

func TestDirectoryInvalidNodeGroupAndFindByNode(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	node1 := registerTestNode(t, dir, "game-1", "session-a")
	node2 := registerTestNode(t, dir, "game-2", "session-b")

	if err := dir.NodeRegistry().MarkNodeInvalid(ctx, "game", "default", "game-1"); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
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
	if placement.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("allocated invalid node %q, want %q", placement.NodeIdentity, node2.NodeIdentity)
	}

	replacement := node1
	replacement.NodeSessionID = "session-new"
	if _, err := dir.NodeRegistry().ReplaceNodeSession(ctx, replacement); err != nil {
		t.Fatalf("ReplaceNodeSession error: %v", err)
	}
	if nodes, err := dir.effectiveNodes(ctx, "game", "default"); err != nil || len(nodes) != 1 || nodes[0].NodeIdentity != node2.NodeIdentity {
		t.Fatalf("effective nodes = %+v, err = %v", nodes, err)
	}

	page, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: node2.NodeIdentity, Limit: 10})
	if err != nil {
		t.Fatalf("FindByNode error: %v", err)
	}
	if len(page.Placements) != 1 || page.Placements[0].GrainKey != placement.GrainKey {
		t.Fatalf("FindByNode page = %+v", page)
	}
}

func TestDirectoryRecoverAndFindByNodePagination(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	node1 := registerTestNode(t, dir, "game-1", "session-a")
	node2 := registerTestNode(t, dir, "game-2", "session-b")

	first, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate first error: %v", err)
	}
	recovered, err := dir.Recover(ctx, sp.RecoverCommand{
		GrainKey:         first.GrainKey,
		NewNodeIdentity:  node2.NodeIdentity,
		PlacementVersion: first.Version,
		LeaseTTL:         time.Minute,
	})
	if err != nil {
		t.Fatalf("Recover error: %v", err)
	}
	if recovered.NodeIdentity != node2.NodeIdentity || recovered.Version != first.Version+1 {
		t.Fatalf("recovered = %+v", recovered)
	}
	if err := dir.NodeRegistry().MarkNodeInvalid(ctx, "game", "default", "game-2"); err != nil {
		t.Fatalf("MarkNodeInvalid game-2 error: %v", err)
	}

	for _, id := range []string{"10002", "10003", "10004"} {
		_, err := dir.Allocate(ctx, sp.AllocateCommand{
			GrainID:         id,
			Kind:            "Player",
			TargetNodeType:  "game",
			TargetNodeGroup: "default",
			LeaseTTL:        time.Minute,
		})
		if err != nil {
			t.Fatalf("Allocate %s error: %v", id, err)
		}
	}

	page, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: node1.NodeIdentity, Limit: 1})
	if err != nil {
		t.Fatalf("FindByNode page1 error: %v", err)
	}
	if len(page.Placements) != 1 || page.NextCursor == "" {
		t.Fatalf("page1 = %+v", page)
	}
	next, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: node1.NodeIdentity, Cursor: page.NextCursor, Limit: 10})
	if err != nil {
		t.Fatalf("FindByNode page2 error: %v", err)
	}
	if len(next.Placements) == 0 {
		t.Fatalf("page2 = %+v", next)
	}
}

func TestDirectoryExpireRemovesPlacementAndPublishesLeaseExpired(t *testing.T) {
	ctx := context.Background()
	dir, bus := newTestDirectory(t)
	node := registerTestNode(t, dir, "game-1", "session-a")
	var seen []sp.EventType
	_ = bus.Subscribe(ctx, func(event sp.PlacementEvent) error {
		seen = append(seen, event.Type)
		return nil
	})

	placement, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	if err := dir.Expire(ctx, sp.ExpireCommand{
		GrainKey:     placement.GrainKey,
		LeaseVersion: placement.Lease.Version,
		Now:          time.Now().Add(time.Second),
	}); err != nil {
		t.Fatalf("Expire error: %v", err)
	}
	if _, err := dir.Lookup(ctx, placement.GrainKey); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Lookup after expire err = %v", err)
	}
	page, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: node.NodeIdentity, Limit: 10})
	if err != nil {
		t.Fatalf("FindByNode error: %v", err)
	}
	if len(page.Placements) != 0 {
		t.Fatalf("FindByNode after expire = %+v", page)
	}
	if seen[len(seen)-1] != sp.EventLeaseExpired {
		t.Fatalf("last event = %v", seen)
	}
}

func TestDirectoryRecoverRejectsReleasedPlacement(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	node := registerTestNode(t, dir, "game-1", "session-a")
	registerTestNode(t, dir, "game-2", "session-b")

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
	if err := dir.Release(ctx, sp.ReleaseCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
	}); err != nil {
		t.Fatalf("Release error: %v", err)
	}
	_, err = dir.Recover(ctx, sp.RecoverCommand{
		GrainKey:         placement.GrainKey,
		NewNodeIdentity:  "game/default/game-2",
		PlacementVersion: placement.Version + 1,
		LeaseTTL:         time.Minute,
	})
	if !errors.Is(err, sp.ErrPlacementNotRecoverable) {
		t.Fatalf("Recover after release err = %v, want ErrPlacementNotRecoverable", err)
	}
}

func TestDirectoryRecoverAfterExpire(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	registerTestNode(t, dir, "game-1", "session-a")
	node2 := registerTestNode(t, dir, "game-2", "session-b")

	placement, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	if err := dir.Expire(ctx, sp.ExpireCommand{
		GrainKey:     placement.GrainKey,
		LeaseVersion: placement.Lease.Version,
		Now:          time.Now().Add(time.Second),
	}); err != nil {
		t.Fatalf("Expire error: %v", err)
	}
	recovered, err := dir.Recover(ctx, sp.RecoverCommand{
		GrainKey:         placement.GrainKey,
		NewNodeIdentity:  node2.NodeIdentity,
		PlacementVersion: placement.Version + 1,
		LeaseTTL:         time.Minute,
	})
	if err != nil {
		t.Fatalf("Recover after expire error: %v", err)
	}
	if recovered.Status != sp.PlacementStatusActive || recovered.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("recovered = %+v", recovered)
	}
}

func TestDirectoryAllocateConcurrentSameGrain(t *testing.T) {
	ctx := context.Background()
	dir, bus := newTestDirectory(t)
	registerTestNode(t, dir, "game-1", "session-a")

	var created int
	_ = bus.Subscribe(ctx, func(event sp.PlacementEvent) error {
		if event.Type == sp.EventPlacementCreated {
			created++
		}
		return nil
	})

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := dir.Allocate(ctx, sp.AllocateCommand{
				GrainID:         "10001",
				Kind:            "Player",
				TargetNodeType:  "game",
				TargetNodeGroup: "default",
				LeaseTTL:        time.Minute,
			})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Allocate error: %v", err)
		}
	}
	key, _ := sp.NewGrainKey("Player", "10001")
	if ok, err := dir.Exists(ctx, key); err != nil || !ok {
		t.Fatalf("Exists after concurrent allocate = %v, err = %v", ok, err)
	}
	if created != 1 {
		t.Fatalf("PlacementCreated events = %d, want 1", created)
	}
}
