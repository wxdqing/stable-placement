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

type blockingStrategy struct {
	started chan struct{}
	release chan struct{}
}

func (s blockingStrategy) Choose(_ context.Context, input sp.StrategyInput) (sp.Node, error) {
	close(s.started)
	<-s.release
	return input.EffectiveNodes[0], nil
}

func newTestDirectory(t *testing.T) (*Directory, *fakeClock, *recordingPublisher) {
	t.Helper()
	clock := newFakeClock(time.Unix(1_000, 0))
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, time.Minute)
	directory, err := NewDirectory(registry, sp.StrategyModeGo, strategies.NewRoundRobin(), publisher)
	if err != nil {
		t.Fatalf("NewDirectory error: %v", err)
	}
	return directory, clock, publisher
}

func registerTestNode(t *testing.T, directory *Directory, name, session string) sp.Node {
	t.Helper()
	node := testNode(name, session)
	if err := directory.NodeRegistry().RegisterNode(context.Background(), node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	return node
}

func allocateTestPlacement(t *testing.T, directory *Directory, grainID string, node sp.Node) *sp.Placement {
	t.Helper()
	placement, err := directory.Allocate(context.Background(), sp.AllocateCommand{
		GrainID: grainID, Kind: "Player", TargetNodeType: node.NodeType, TargetNodeGroup: node.NodeGroup,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	return placement
}

func TestDirectoryLookupAndExistsRequireUsableOwnerLease(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*Directory, sp.Node, *fakeClock)
		wantErr error
	}{
		{name: "active"},
		{name: "draining", mutate: func(d *Directory, n sp.Node, _ *fakeClock) {
			setNodeStatus(d.registry, n.NodeIdentity, sp.NodeStatusDraining)
		}},
		{name: "missing", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { deleteNode(d.registry, n.NodeIdentity) }, wantErr: sp.ErrPlacementNotFound},
		{name: "offline", mutate: func(d *Directory, n sp.Node, _ *fakeClock) {
			setNodeStatus(d.registry, n.NodeIdentity, sp.NodeStatusOffline)
		}, wantErr: sp.ErrPlacementNotFound},
		{name: "session mismatch", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { setNodeSession(d.registry, n.NodeIdentity, "session-b") }, wantErr: sp.ErrPlacementNotFound},
		{name: "expiry boundary", mutate: func(_ *Directory, _ sp.Node, c *fakeClock) { c.Advance(time.Minute) }, wantErr: sp.ErrPlacementNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, clock, _ := newTestDirectory(t)
			node := registerTestNode(t, directory, "game-1", "session-a")
			placement := allocateTestPlacement(t, directory, "10001", node)
			if test.mutate != nil {
				test.mutate(directory, node, clock)
			}
			route, err := directory.Lookup(context.Background(), placement.GrainKey)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Lookup err = %v, want %v", err, test.wantErr)
			}
			exists, err := directory.Exists(context.Background(), placement.GrainKey)
			if err != nil || exists != (test.wantErr == nil) {
				t.Fatalf("Exists = %v, %v", exists, err)
			}
			if test.wantErr == nil {
				nodeState, _ := directory.registry.Node(node.NodeIdentity)
				if route.GrainKey != placement.GrainKey || route.OwnerNodeSessionID != node.NodeSessionID || route.NodeLeaseVersion != nodeState.Lease.Version || !route.ValidUntil.Equal(time.UnixMilli(nodeState.Lease.ExpireAtUnixMilli)) {
					t.Fatalf("route = %+v, node = %+v", route, nodeState)
				}
			} else if _, ok := directory.placements[placement.GrainKey]; !ok {
				t.Fatal("logical expiry removed placement")
			}
		})
	}
}

func TestDirectoryLookupRejectsReleasedPlacement(t *testing.T) {
	directory, _, _ := newTestDirectory(t)
	node := registerTestNode(t, directory, "game-1", "session-a")
	placement := allocateTestPlacement(t, directory, "10001", node)
	if err := directory.Release(context.Background(), sp.ReleaseCommand{GrainKey: placement.GrainKey, NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: placement.Version}); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.Lookup(context.Background(), placement.GrainKey); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Lookup released err = %v", err)
	}
}

func TestDirectoryAllocateFiltersNodeLeaseAndInvalidState(t *testing.T) {
	directory, clock, _ := newTestDirectory(t)
	active := registerTestNode(t, directory, "active", "s")
	offline := registerTestNode(t, directory, "offline", "s")
	expired := registerTestNode(t, directory, "expired", "s")
	invalid := registerTestNode(t, directory, "invalid", "s")
	setNodeStatus(directory.registry, offline.NodeIdentity, sp.NodeStatusOffline)
	setNodeExpiry(directory.registry, expired.NodeIdentity, clock.Now().UnixMilli())
	if err := directory.registry.MarkNodeInvalid(context.Background(), invalid.NodeType, invalid.NodeGroup, invalid.NodeName); err != nil {
		t.Fatal(err)
	}

	placement, err := directory.Allocate(context.Background(), sp.AllocateCommand{GrainID: "10001", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	if placement.NodeIdentity != active.NodeIdentity || placement.OwnerNodeSessionID != active.NodeSessionID {
		t.Fatalf("placement = %+v", placement)
	}
}

func TestDirectoryAllocateExistingPlacementRules(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*Directory, sp.Node, *fakeClock)
		wantErr error
	}{
		{name: "healthy"},
		{name: "missing", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { deleteNode(d.registry, n.NodeIdentity) }, wantErr: sp.ErrPlacementOwnerUnavailable},
		{name: "offline", mutate: func(d *Directory, n sp.Node, _ *fakeClock) {
			setNodeStatus(d.registry, n.NodeIdentity, sp.NodeStatusOffline)
		}, wantErr: sp.ErrPlacementOwnerUnavailable},
		{name: "expired", mutate: func(d *Directory, n sp.Node, c *fakeClock) {
			setNodeExpiry(d.registry, n.NodeIdentity, c.Now().UnixMilli())
		}, wantErr: sp.ErrPlacementOwnerUnavailable},
		{name: "session mismatch", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { setNodeSession(d.registry, n.NodeIdentity, "new") }, wantErr: sp.ErrPlacementOwnerUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, clock, _ := newTestDirectory(t)
			node := registerTestNode(t, directory, "game-1", "session-a")
			first := allocateTestPlacement(t, directory, "10001", node)
			before := *first
			if test.mutate != nil {
				test.mutate(directory, node, clock)
			}
			got, err := directory.Allocate(context.Background(), sp.AllocateCommand{GrainID: "10001", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Allocate err = %v, want %v", err, test.wantErr)
			}
			if test.wantErr == nil && *got != before {
				t.Fatalf("idempotent placement = %+v, want %+v", got, before)
			}
			if directory.placements[first.GrainKey] != before || len(directory.byNode[node.NodeIdentity]) != 1 {
				t.Fatal("failed Allocate changed placement or index")
			}
		})
	}
}

func TestDirectoryAllocateReleasedPlacementAdvancesHistory(t *testing.T) {
	directory, _, _ := newTestDirectory(t)
	node := registerTestNode(t, directory, "game-1", "session-a")
	first := allocateTestPlacement(t, directory, "10001", node)
	if err := directory.Release(context.Background(), sp.ReleaseCommand{GrainKey: first.GrainKey, NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: first.Version}); err != nil {
		t.Fatal(err)
	}
	second := allocateTestPlacement(t, directory, "10001", node)
	if second.Version != first.Version+2 || second.Status != sp.PlacementStatusActive {
		t.Fatalf("reallocated placement = %+v, first = %+v", second, first)
	}
}

func TestDirectoryRenewValidatesOwnerLeaseAndOnlyAudits(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*Directory, sp.Node, *fakeClock)
		command func(*sp.Placement, sp.Node) sp.RenewCommand
		wantErr error
	}{
		{name: "active"},
		{name: "draining", mutate: func(d *Directory, n sp.Node, _ *fakeClock) {
			setNodeStatus(d.registry, n.NodeIdentity, sp.NodeStatusDraining)
		}},
		{name: "missing", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { deleteNode(d.registry, n.NodeIdentity) }, wantErr: sp.ErrPlacementOwnerUnavailable},
		{name: "offline", mutate: func(d *Directory, n sp.Node, _ *fakeClock) {
			setNodeStatus(d.registry, n.NodeIdentity, sp.NodeStatusOffline)
		}, wantErr: sp.ErrPlacementOwnerUnavailable},
		{name: "expired", mutate: func(d *Directory, n sp.Node, c *fakeClock) {
			setNodeExpiry(d.registry, n.NodeIdentity, c.Now().UnixMilli())
		}, wantErr: sp.ErrNodeLeaseExpired},
		{name: "session mismatch", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { setNodeSession(d.registry, n.NodeIdentity, "new") }, wantErr: sp.ErrInvalidNodeSession},
		{name: "version", command: func(p *sp.Placement, n sp.Node) sp.RenewCommand {
			return sp.RenewCommand{GrainKey: p.GrainKey, NodeIdentity: n.NodeIdentity, NodeSessionID: n.NodeSessionID, PlacementVersion: p.Version + 1}
		}, wantErr: sp.ErrVersionConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, clock, publisher := newTestDirectory(t)
			node := registerTestNode(t, directory, "game-1", "session-a")
			placement := allocateTestPlacement(t, directory, "10001", node)
			beforePlacement := directory.placements[placement.GrainKey]
			beforeNode, _ := directory.registry.Node(node.NodeIdentity)
			if test.mutate != nil {
				test.mutate(directory, node, clock)
				beforeNode, _ = directory.registry.Node(node.NodeIdentity)
			}
			cmd := sp.RenewCommand{GrainKey: placement.GrainKey, NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: placement.Version}
			if test.command != nil {
				cmd = test.command(placement, node)
			}
			eventsBefore := len(publisher.Events())
			got, err := directory.Renew(context.Background(), cmd)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Renew err = %v, want %v", err, test.wantErr)
			}
			if directory.placements[placement.GrainKey] != beforePlacement {
				t.Fatal("Renew changed placement")
			}
			afterNode, _ := directory.registry.Node(node.NodeIdentity)
			if afterNode != beforeNode {
				t.Fatal("Renew changed node lease")
			}
			if test.wantErr == nil {
				if *got != beforePlacement || len(publisher.Events()) != eventsBefore+1 || publisher.Events()[eventsBefore].Type != sp.EventPlacementRenewed {
					t.Fatalf("Renew result/events = %+v / %+v", got, publisher.Events()[eventsBefore:])
				}
			}
		})
	}
}

func TestDirectoryRenewReturnsAuditFailureWithoutStateChange(t *testing.T) {
	directory, _, publisher := newTestDirectory(t)
	node := registerTestNode(t, directory, "game-1", "session-a")
	placement := allocateTestPlacement(t, directory, "10001", node)
	beforePlacement := directory.placements[placement.GrainKey]
	beforeNode, _ := directory.registry.Node(node.NodeIdentity)
	wantErr := errors.New("audit failed")
	publisher.err = wantErr
	_, err := directory.Renew(context.Background(), sp.RenewCommand{GrainKey: placement.GrainKey, NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: placement.Version})
	if !errors.Is(err, wantErr) || directory.placements[placement.GrainKey] != beforePlacement {
		t.Fatalf("Renew err=%v placement=%+v", err, directory.placements[placement.GrainKey])
	}
	afterNode, _ := directory.registry.Node(node.NodeIdentity)
	if afterNode != beforeNode {
		t.Fatal("failed audit changed node")
	}
}

func TestDirectoryReleaseOwnerRules(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*Directory, sp.Node, *fakeClock)
		wantErr error
	}{
		{name: "active"},
		{name: "offline", mutate: func(d *Directory, n sp.Node, _ *fakeClock) {
			setNodeStatus(d.registry, n.NodeIdentity, sp.NodeStatusOffline)
		}},
		{name: "expired", mutate: func(d *Directory, n sp.Node, c *fakeClock) {
			setNodeExpiry(d.registry, n.NodeIdentity, c.Now().UnixMilli())
		}},
		{name: "replaced session", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { setNodeSession(d.registry, n.NodeIdentity, "new") }, wantErr: sp.ErrInvalidNodeSession},
		{name: "missing", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { deleteNode(d.registry, n.NodeIdentity) }, wantErr: sp.ErrPlacementOwnerUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, clock, _ := newTestDirectory(t)
			node := registerTestNode(t, directory, "game-1", "session-a")
			placement := allocateTestPlacement(t, directory, "10001", node)
			if test.mutate != nil {
				test.mutate(directory, node, clock)
			}
			err := directory.Release(context.Background(), sp.ReleaseCommand{GrainKey: placement.GrainKey, NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: placement.Version})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Release err = %v, want %v", err, test.wantErr)
			}
			got := directory.placements[placement.GrainKey]
			if test.wantErr == nil && (got.Status != sp.PlacementStatusReleased || got.Version != placement.Version+1 || len(directory.byNode[node.NodeIdentity]) != 0) {
				t.Fatalf("released placement/index = %+v / %+v", got, directory.byNode)
			}
			if test.wantErr != nil && got != *placement {
				t.Fatal("failed Release changed placement")
			}
		})
	}
}

func TestDirectoryTransferAndRecoverUseHealthyTargetSession(t *testing.T) {
	directory, clock, _ := newTestDirectory(t)
	owner := registerTestNode(t, directory, "owner", "owner-session")
	target := registerTestNode(t, directory, "target", "target-session")
	badTarget := registerTestNode(t, directory, "bad", "bad-session")
	setNodeExpiry(directory.registry, badTarget.NodeIdentity, clock.Now().UnixMilli())
	placement := allocateTestPlacement(t, directory, "10001", owner)

	if _, err := directory.Transfer(context.Background(), sp.TransferCommand{GrainKey: placement.GrainKey, ToNodeIdentity: badTarget.NodeIdentity, PlacementVersion: placement.Version}); !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("Transfer unhealthy target err = %v", err)
	}
	transferred, err := directory.Transfer(context.Background(), sp.TransferCommand{GrainKey: placement.GrainKey, FromNodeIdentity: owner.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: placement.Version})
	if err != nil {
		t.Fatalf("Transfer error: %v", err)
	}
	if transferred.NodeIdentity != target.NodeIdentity || transferred.OwnerNodeSessionID != target.NodeSessionID || transferred.Version != placement.Version+1 {
		t.Fatalf("transferred = %+v", transferred)
	}
	if _, err := directory.Recover(context.Background(), sp.RecoverCommand{GrainKey: transferred.GrainKey, NewNodeIdentity: owner.NodeIdentity, PlacementVersion: transferred.Version}); !errors.Is(err, sp.ErrPlacementNotRecoverable) {
		t.Fatalf("Recover healthy owner err = %v", err)
	}
	setNodeSession(directory.registry, target.NodeIdentity, "replacement")
	recovered, err := directory.Recover(context.Background(), sp.RecoverCommand{GrainKey: transferred.GrainKey, NewNodeIdentity: owner.NodeIdentity, PlacementVersion: transferred.Version})
	if err != nil {
		t.Fatalf("Recover unavailable owner error: %v", err)
	}
	if recovered.NodeIdentity != owner.NodeIdentity || recovered.OwnerNodeSessionID != owner.NodeSessionID || recovered.Version != transferred.Version+1 {
		t.Fatalf("recovered = %+v", recovered)
	}
}

func TestDirectoryRecoverAllowsEveryUnavailableOwnerKind(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Directory, sp.Node, *fakeClock)
	}{
		{name: "missing", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { deleteNode(d.registry, n.NodeIdentity) }},
		{name: "offline", mutate: func(d *Directory, n sp.Node, _ *fakeClock) {
			setNodeStatus(d.registry, n.NodeIdentity, sp.NodeStatusOffline)
		}},
		{name: "expired", mutate: func(d *Directory, n sp.Node, c *fakeClock) {
			setNodeExpiry(d.registry, n.NodeIdentity, c.Now().UnixMilli())
		}},
		{name: "session mismatch", mutate: func(d *Directory, n sp.Node, _ *fakeClock) { setNodeSession(d.registry, n.NodeIdentity, "new") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, clock, _ := newTestDirectory(t)
			owner := registerTestNode(t, directory, "owner", "old")
			target := registerTestNode(t, directory, "target", "target")
			placement := allocateTestPlacement(t, directory, "10001", owner)
			test.mutate(directory, owner, clock)
			if _, err := directory.Recover(context.Background(), sp.RecoverCommand{GrainKey: placement.GrainKey, NewNodeIdentity: target.NodeIdentity, PlacementVersion: placement.Version}); err != nil {
				t.Fatalf("Recover error: %v", err)
			}
		})
	}
}

func TestDirectoryFindByNodeRejectsNegativeAndAllowsPastEndCursor(t *testing.T) {
	directory, _, _ := newTestDirectory(t)
	node := registerTestNode(t, directory, "game-1", "session-a")
	allocateTestPlacement(t, directory, "10001", node)
	if _, err := directory.FindByNode(context.Background(), sp.FindByNodeQuery{NodeIdentity: node.NodeIdentity, Cursor: "-1"}); err == nil {
		t.Fatal("negative cursor succeeded")
	}
	page, err := directory.FindByNode(context.Background(), sp.FindByNodeQuery{NodeIdentity: node.NodeIdentity, Cursor: "10"})
	if err != nil || len(page.Placements) != 0 || page.NextCursor != "" {
		t.Fatalf("past-end page = %+v, %v", page, err)
	}
}

func TestDirectoryCompleteDrainRejectsNodeWithPlacements(t *testing.T) {
	directory, _, _ := newTestDirectory(t)
	node := registerTestNode(t, directory, "game-1", "session-a")
	placement := allocateTestPlacement(t, directory, "10001", node)
	if err := directory.registry.MarkNodeInvalid(context.Background(), node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatal(err)
	}
	if err := directory.registry.DrainNode(context.Background(), node.NodeIdentity); err != nil {
		t.Fatal(err)
	}
	if err := directory.registry.CompleteDrain(context.Background(), node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeHasPlacements) {
		t.Fatalf("CompleteDrain err = %v", err)
	}
	if err := directory.Release(context.Background(), sp.ReleaseCommand{GrainKey: placement.GrainKey, NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: placement.Version}); err != nil {
		t.Fatal(err)
	}
	if err := directory.registry.CompleteDrain(context.Background(), node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatalf("CompleteDrain after release: %v", err)
	}
}

func TestDirectoryCompleteDrainPreventsConcurrentAllocateCommit(t *testing.T) {
	clock := newFakeClock(time.Unix(2_000, 0))
	registry := newTestRegistry(t, clock, nil, time.Minute)
	strategy := blockingStrategy{started: make(chan struct{}), release: make(chan struct{})}
	directory, err := NewDirectory(registry, sp.StrategyModeGo, strategy, nil)
	if err != nil {
		t.Fatal(err)
	}
	node := registerTestNode(t, directory, "game-1", "session-a")
	done := make(chan error, 1)
	go func() {
		_, err := directory.Allocate(context.Background(), sp.AllocateCommand{GrainID: "10001", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
		done <- err
	}()
	<-strategy.started
	if err := registry.MarkNodeInvalid(context.Background(), node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatal(err)
	}
	if err := registry.DrainNode(context.Background(), node.NodeIdentity); err != nil {
		t.Fatal(err)
	}
	if err := registry.CompleteDrain(context.Background(), node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatal(err)
	}
	close(strategy.release)
	if err := <-done; !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("Allocate err = %v", err)
	}
}

func TestDirectoryConcurrentOperationsFollowDirectoryThenRegistryLockOrder(t *testing.T) {
	directory, _, _ := newTestDirectory(t)
	node := registerTestNode(t, directory, "game-1", "session-a")
	placement := allocateTestPlacement(t, directory, "10001", node)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = directory.Lookup(context.Background(), placement.GrainKey)
		}()
		go func() {
			defer wg.Done()
			_ = registryRenew(directory.registry, node)
		}()
	}
	wg.Wait()
}

func registryRenew(registry *NodeRegistry, node sp.Node) error {
	return registry.RenewNode(context.Background(), node.NodeIdentity, node.NodeSessionID)
}

func setNodeStatus(registry *NodeRegistry, identity string, status sp.NodeStatus) {
	registry.mu.Lock()
	node := registry.nodes[identity]
	node.Status = status
	registry.nodes[identity] = node
	registry.mu.Unlock()
}

func setNodeSession(registry *NodeRegistry, identity, session string) {
	registry.mu.Lock()
	node := registry.nodes[identity]
	node.NodeSessionID = session
	registry.nodes[identity] = node
	registry.mu.Unlock()
}

func setNodeExpiry(registry *NodeRegistry, identity string, expiry int64) {
	registry.mu.Lock()
	node := registry.nodes[identity]
	node.Lease.ExpireAtUnixMilli = expiry
	registry.nodes[identity] = node
	registry.mu.Unlock()
}

func deleteNode(registry *NodeRegistry, identity string) {
	registry.mu.Lock()
	delete(registry.nodes, identity)
	registry.mu.Unlock()
}
