package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type recordingPublisher struct {
	mu     sync.Mutex
	events []sp.PlacementEvent
	err    error
}

func (p *recordingPublisher) Publish(_ context.Context, event sp.PlacementEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return p.err
}

func (p *recordingPublisher) Events() []sp.PlacementEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sp.PlacementEvent(nil), p.events...)
}

func newTestRegistry(t *testing.T, clock *fakeClock, publisher sp.EventPublisher, ttl time.Duration) *NodeRegistry {
	t.Helper()
	registry, err := newNodeRegistry(publisher, sp.NodeLeaseConfig{TTL: ttl}, clock.Now)
	if err != nil {
		t.Fatalf("newNodeRegistry error: %v", err)
	}
	return registry
}

func testNode(name, session string) sp.Node {
	return sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      name,
		NodeIdentity:  "game/default/" + name,
		NodeSessionID: session,
		Status:        sp.NodeStatusActive,
	}
}

func TestMemoryNodeLeaseConfigRejectsNonPositiveTTLAndDefaultsToMinute(t *testing.T) {
	if sp.DefaultNodeLeaseConfig().TTL != time.Minute {
		t.Fatalf("default TTL = %v, want 1m", sp.DefaultNodeLeaseConfig().TTL)
	}
	for _, test := range []struct {
		name    string
		config  sp.NodeLeaseConfig
		wantErr error
	}{
		{name: "default", config: sp.DefaultNodeLeaseConfig()},
		{name: "positive", config: sp.NodeLeaseConfig{TTL: time.Second}},
		{name: "zero", config: sp.NodeLeaseConfig{}, wantErr: sp.ErrInvalidNodeLeaseTTL},
		{name: "negative", config: sp.NodeLeaseConfig{TTL: -time.Second}, wantErr: sp.ErrInvalidNodeLeaseTTL},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry, err := NewNodeRegistry(nil, test.config)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("NewNodeRegistry err = %v, want %v", err, test.wantErr)
			}
			if test.wantErr == nil && registry == nil {
				t.Fatal("NewNodeRegistry returned nil registry")
			}
		})
	}
}

func TestNodeRegistryRoundsPositiveSubMillisecondTTLUp(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newFakeClock(start)
	registry := newTestRegistry(t, clock, nil, 500*time.Microsecond)
	node := testNode("game-1", "session-a")

	if _, err := registry.RegisterNode(context.Background(), node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	got, ok := registry.Node(node.NodeIdentity)
	if !ok || got.Lease.TTLMillis != 1 || got.Lease.ExpireAtUnixMilli != start.Add(time.Millisecond).UnixMilli() {
		t.Fatalf("registered node = %+v, ok=%v", got, ok)
	}
}

func TestNodeRegistryCapsTTLAtLargestWholeMillisecond(t *testing.T) {
	start := time.Unix(0, 0)
	clock := newFakeClock(start)
	maxTTL := time.Duration(1<<63 - 1)
	registry, err := newNodeRegistry(nil, sp.NodeLeaseConfig{TTL: maxTTL}, clock.Now)
	if err != nil {
		t.Fatalf("newNodeRegistry error: %v", err)
	}
	node := testNode("game-1", "session-a")

	if _, err := registry.RegisterNode(context.Background(), node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	got, ok := registry.Node(node.NodeIdentity)
	wantTTLMillis := maxTTL.Truncate(time.Millisecond).Milliseconds()
	if !ok || got.Lease.TTLMillis <= 0 || got.Lease.TTLMillis != wantTTLMillis || got.Lease.ExpireAtUnixMilli <= start.UnixMilli() {
		t.Fatalf("registered node = %+v, ok=%v, want TTLMillis=%d and future expiry", got, ok, wantTTLMillis)
	}
}

func TestNodeRegistryRegisterLeaseRules(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(100, 0)
	clock := newFakeClock(start)
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, 10*time.Second)
	node := testNode("game-1", "session-a")
	node.Metrics = sp.NodeMetrics{CPUAvailableMilliCores: 500, UpdatedAtUnixMilli: 1}
	node.Status = sp.NodeStatusDraining

	if _, err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	got, ok := registry.Node(node.NodeIdentity)
	if !ok || got.Status != sp.NodeStatusActive || got.Lease.Version != 1 || got.Lease.TTLMillis != 10_000 || got.Lease.ExpireAtUnixMilli != start.Add(10*time.Second).UnixMilli() || got.Metrics != (sp.NodeMetrics{}) {
		t.Fatalf("registered node = %+v, ok=%v", got, ok)
	}

	clock.Advance(time.Second)
	if _, err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatalf("idempotent RegisterNode error: %v", err)
	}
	idempotent, _ := registry.Node(node.NodeIdentity)
	if idempotent.Lease != got.Lease || len(publisher.Events()) != 1 {
		t.Fatalf("idempotent register changed lease/events: lease=%+v events=%d", idempotent.Lease, len(publisher.Events()))
	}

	registry.mu.Lock()
	offline := registry.nodes[node.NodeIdentity]
	offline.Status = sp.NodeStatusOffline
	registry.nodes[node.NodeIdentity] = offline
	registry.mu.Unlock()
	if _, err := registry.RegisterNode(ctx, node); !errors.Is(err, sp.ErrNodeLeaseExpired) {
		t.Fatalf("offline RegisterNode err = %v", err)
	}

	registry.mu.Lock()
	expired := registry.nodes[node.NodeIdentity]
	expired.Status = sp.NodeStatusActive
	expired.Lease.ExpireAtUnixMilli = clock.Now().UnixMilli()
	registry.nodes[node.NodeIdentity] = expired
	registry.mu.Unlock()
	if _, err := registry.RegisterNode(ctx, node); !errors.Is(err, sp.ErrNodeLeaseExpired) {
		t.Fatalf("expired RegisterNode err = %v", err)
	}
}

func TestNodeRegistryIdentityMismatchDoesNotWriteStateOrEvents(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, time.Second)
	for _, node := range []sp.Node{
		{NodeGroup: "default", NodeName: "game-1", NodeIdentity: "game/default/game-1", NodeSessionID: "s"},
		{NodeType: "game", NodeGroup: "default", NodeName: "game-1", NodeSessionID: "s"},
		{NodeType: "game", NodeGroup: "default", NodeName: "game-1", NodeIdentity: "wrong", NodeSessionID: "s"},
	} {
		if _, err := registry.RegisterNode(context.Background(), node); err == nil {
			t.Fatalf("RegisterNode(%+v) succeeded", node)
		}
	}
	if len(registry.nodes) != 0 || len(publisher.Events()) != 0 {
		t.Fatalf("invalid registrations changed state/events: nodes=%d events=%d", len(registry.nodes), len(publisher.Events()))
	}

	valid := testNode("game-1", "session-a")
	if _, err := registry.RegisterNode(context.Background(), valid); err != nil {
		t.Fatal(err)
	}
	before, _ := registry.Node(valid.NodeIdentity)
	eventsBefore := len(publisher.Events())
	mismatch := valid
	mismatch.NodeIdentity = "wrong"
	mismatch.NodeSessionID = "session-b"
	if _, _, err := registry.ReplaceNodeSession(context.Background(), mismatch); err == nil {
		t.Fatal("identity-mismatch ReplaceNodeSession succeeded")
	}
	after, _ := registry.Node(valid.NodeIdentity)
	if after != before || len(registry.nodes) != 1 || len(publisher.Events()) != eventsBefore {
		t.Fatal("identity-mismatch replacement changed state or events")
	}
}

func TestNodeRegistryRenewLeaseRules(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(200, 0)
	clock := newFakeClock(start)
	registry := newTestRegistry(t, clock, nil, 10*time.Second)
	node := testNode("game-1", "session-a")
	if _, err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}

	clock.Advance(2 * time.Second)
	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
		t.Fatalf("active RenewNode error: %v", err)
	}
	renewed, _ := registry.Node(node.NodeIdentity)
	if renewed.Lease.Version != 2 || renewed.Lease.ExpireAtUnixMilli != clock.Now().Add(10*time.Second).UnixMilli() {
		t.Fatalf("renewed lease = %+v", renewed.Lease)
	}

	registry.mu.Lock()
	draining := registry.nodes[node.NodeIdentity]
	draining.Status = sp.NodeStatusDraining
	draining.Lease.TTLMillis = 20_000
	registry.nodes[node.NodeIdentity] = draining
	registry.mu.Unlock()
	clock.Advance(time.Second)
	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
		t.Fatalf("draining RenewNode error: %v", err)
	}
	renewed, _ = registry.Node(node.NodeIdentity)
	if renewed.Lease.Version != 3 || renewed.Lease.ExpireAtUnixMilli != clock.Now().Add(20*time.Second).UnixMilli() {
		t.Fatalf("draining renewed lease = %+v", renewed.Lease)
	}

	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: "old-session"}); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("old session err = %v", err)
	}
	registry.mu.Lock()
	offline := registry.nodes[node.NodeIdentity]
	offline.Status = sp.NodeStatusOffline
	registry.nodes[node.NodeIdentity] = offline
	registry.mu.Unlock()
	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); !errors.Is(err, sp.ErrNodeNotFound) {
		t.Fatalf("offline err = %v", err)
	}
	registry.mu.Lock()
	expired := registry.nodes[node.NodeIdentity]
	expired.Status = sp.NodeStatusActive
	expired.Lease.ExpireAtUnixMilli = clock.Now().UnixMilli()
	registry.nodes[node.NodeIdentity] = expired
	registry.mu.Unlock()
	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); !errors.Is(err, sp.ErrNodeLeaseExpired) {
		t.Fatalf("expired err = %v", err)
	}
}

func TestNodeRegistryRenewMetricsAtomically(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(250, 0))
	registry := newTestRegistry(t, clock, nil, 10*time.Second)
	node := testNode("game-1", "session-a")
	if _, err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}

	metrics := sp.NodeMetrics{CPUAvailableMilliCores: 500, MemoryAvailableBytes: 1 << 30, Goroutines: 20, UpdatedAtUnixMilli: 1}
	clock.Advance(time.Second)
	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, Metrics: &metrics}); err != nil {
		t.Fatal(err)
	}
	withMetrics, _ := registry.Node(node.NodeIdentity)
	if withMetrics.Metrics.CPUAvailableMilliCores != 500 || withMetrics.Metrics.MemoryAvailableBytes != 1<<30 || withMetrics.Metrics.Goroutines != 20 || withMetrics.Metrics.UpdatedAtUnixMilli != clock.Now().UnixMilli() {
		t.Fatalf("renewed metrics = %+v", withMetrics.Metrics)
	}

	clock.Advance(time.Second)
	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
		t.Fatal(err)
	}
	withoutMetrics, _ := registry.Node(node.NodeIdentity)
	if withoutMetrics.Metrics != withMetrics.Metrics || withoutMetrics.Lease.Version != withMetrics.Lease.Version+1 {
		t.Fatalf("nil-metrics renew = %+v", withoutMetrics)
	}

	badMetrics := sp.NodeMetrics{CPUAvailableMilliCores: -1}
	before := withoutMetrics
	for _, cmd := range []sp.RenewNodeCommand{
		{NodeIdentity: node.NodeIdentity, NodeSessionID: "old-session", Metrics: &badMetrics},
		{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, Metrics: &badMetrics},
	} {
		if _, err := registry.RenewNode(ctx, cmd); err == nil {
			t.Fatalf("RenewNode(%+v) succeeded", cmd)
		}
		after, _ := registry.Node(node.NodeIdentity)
		if after != before {
			t.Fatalf("failed renew changed node: before=%+v after=%+v", before, after)
		}
	}

	registry.mu.Lock()
	expired := registry.nodes[node.NodeIdentity]
	expired.Lease.ExpireAtUnixMilli = clock.Now().UnixMilli()
	registry.nodes[node.NodeIdentity] = expired
	registry.mu.Unlock()
	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, Metrics: &metrics}); !errors.Is(err, sp.ErrNodeLeaseExpired) {
		t.Fatalf("expired renew error = %v", err)
	}
	afterExpired, _ := registry.Node(node.NodeIdentity)
	if afterExpired.Metrics != before.Metrics || afterExpired.Lease.Version != before.Lease.Version {
		t.Fatalf("expired renew changed metrics or version: %+v", afterExpired)
	}
}

func TestNodeRegistryConcurrentRenewDoesNotMoveExpiryBackward(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(300, 0))
	registry := newTestRegistry(t, clock, nil, time.Minute)
	node := testNode("game-1", "session-a")
	if _, err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	clock.Set(time.Unix(350, 0))
	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
		t.Fatal(err)
	}
	wantExpiry, _ := registry.Node(node.NodeIdentity)
	clock.Set(time.Unix(340, 0))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
				t.Errorf("RenewNode error: %v", err)
			}
		}()
	}
	wg.Wait()
	got, _ := registry.Node(node.NodeIdentity)
	if got.Lease.ExpireAtUnixMilli < wantExpiry.Lease.ExpireAtUnixMilli {
		t.Fatalf("expiry moved backward: got %d want >= %d", got.Lease.ExpireAtUnixMilli, wantExpiry.Lease.ExpireAtUnixMilli)
	}
}

func TestNodeRegistryRegisterCannotBypassReplaceSessionEvent(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(500, 0))
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, 15*time.Second)
	oldNode := testNode("game-1", "session-a")
	oldNode.Address = "old"
	if _, err := registry.RegisterNode(ctx, oldNode); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkNodeInvalid(ctx, oldNode.NodeType, oldNode.NodeGroup, oldNode.NodeName); err != nil {
		t.Fatal(err)
	}
	eventsBefore := len(publisher.Events())
	otherSession := oldNode
	otherSession.NodeSessionID = "session-b"
	if _, err := registry.RegisterNode(ctx, otherSession); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("different-session RegisterNode err = %v", err)
	}
	unchanged, _ := registry.Node(oldNode.NodeIdentity)
	if unchanged.NodeSessionID != oldNode.NodeSessionID || len(publisher.Events()) != eventsBefore {
		t.Fatal("different-session RegisterNode changed state or events")
	}

	same := oldNode
	same.Address = "changed"
	if _, _, err := registry.ReplaceNodeSession(ctx, same); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("same-session ReplaceNodeSession err = %v", err)
	}
	unchanged, _ = registry.Node(oldNode.NodeIdentity)
	if unchanged.Address != "old" || len(publisher.Events()) != eventsBefore {
		t.Fatalf("same-session replacement changed state/events: node=%+v events=%d", unchanged, len(publisher.Events()))
	}

	clock.Advance(time.Second)
	replacement := oldNode
	replacement.NodeSessionID = "session-b"
	replacement.Status = sp.NodeStatusDraining
	returnedOld, _, err := registry.ReplaceNodeSession(ctx, replacement)
	if err != nil {
		t.Fatalf("ReplaceNodeSession error: %v", err)
	}
	got, _ := registry.Node(oldNode.NodeIdentity)
	if returnedOld.NodeSessionID != "session-a" || got.NodeSessionID != "session-b" || got.Status != sp.NodeStatusActive || got.Lease.Version != 1 || got.Lease.TTLMillis != 15_000 || got.Lease.ExpireAtUnixMilli != clock.Now().Add(15*time.Second).UnixMilli() {
		t.Fatalf("old=%+v replacement=%+v", returnedOld, got)
	}
	if !registry.IsInvalid(oldNode.NodeType, oldNode.NodeGroup, oldNode.NodeName) {
		t.Fatal("invalid group did not survive replacement")
	}
	events := publisher.Events()
	if len(events) != eventsBefore+1 || events[len(events)-1].Type != sp.EventNodeReplaced {
		t.Fatalf("replacement events = %+v", events)
	}
}

func TestNodeRegistryExpireLeasesIsBoundedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(600, 0))
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, time.Second)
	for i, name := range []string{"game-1", "game-2", "game-3"} {
		node := testNode(name, "session-a")
		if _, err := registry.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			registry.mu.Lock()
			n := registry.nodes[node.NodeIdentity]
			n.Status = sp.NodeStatusDraining
			registry.nodes[node.NodeIdentity] = n
			registry.mu.Unlock()
		}
	}
	registeredEvents := len(publisher.Events())
	clock.Advance(time.Second)

	expired, err := registry.ExpireNodeLeases(ctx, "game", "default", 2)
	if err != nil || expired != 2 {
		t.Fatalf("first ExpireNodeLeases = %d, %v", expired, err)
	}
	offline := 0
	for _, node := range registry.nodes {
		if node.Status == sp.NodeStatusOffline {
			offline++
		}
	}
	if offline != 2 || len(registry.nodes) != 3 {
		t.Fatalf("offline=%d tombstones=%d", offline, len(registry.nodes))
	}
	expired, err = registry.ExpireNodeLeases(ctx, "game", "default", 10)
	if err != nil || expired != 1 {
		t.Fatalf("second ExpireNodeLeases = %d, %v", expired, err)
	}
	expired, err = registry.ExpireNodeLeases(ctx, "game", "default", 10)
	if err != nil || expired != 0 {
		t.Fatalf("idempotent ExpireNodeLeases = %d, %v", expired, err)
	}
	events := publisher.Events()[registeredEvents:]
	if len(events) != 3 {
		t.Fatalf("expiry event count = %d, want 3", len(events))
	}
	for _, event := range events {
		if event.Type != sp.EventNodeLeaseExpired || event.NodeSessionID != "session-a" || event.NodeLeaseVersion != 1 {
			t.Fatalf("expiry event = %+v", event)
		}
	}
}

func TestNodeRegistryDrainRejectsExpiredOfflineTombstoneWithoutMutation(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(650, 0))
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, time.Second)
	node := testNode("game-1", "session-a")
	if _, err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkNodeInvalid(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if count, err := registry.ExpireNodeLeases(ctx, node.NodeType, node.NodeGroup, 1); err != nil || count != 1 {
		t.Fatalf("ExpireNodeLeases = %d, %v", count, err)
	}

	beforeNode, ok := registry.Node(node.NodeIdentity)
	if !ok || beforeNode.Status != sp.NodeStatusOffline {
		t.Fatalf("expired node = %+v, ok=%v", beforeNode, ok)
	}
	beforeEvents := publisher.Events()
	if err := registry.DrainNode(ctx, node.NodeIdentity); !errors.Is(err, sp.ErrNodeNotFound) {
		t.Fatalf("DrainNode err = %v", err)
	}
	afterNode, ok := registry.Node(node.NodeIdentity)
	if !ok || afterNode != beforeNode {
		t.Fatalf("node after DrainNode = %+v, want unchanged %+v, ok=%v", afterNode, beforeNode, ok)
	}
	afterEvents := publisher.Events()
	if len(afterEvents) != len(beforeEvents) {
		t.Fatalf("events after DrainNode = %+v, want unchanged %+v", afterEvents, beforeEvents)
	}
	for _, event := range afterEvents {
		if event.Type == sp.EventNodeDraining {
			t.Fatalf("unexpected NodeDraining event: %+v", event)
		}
	}
	if count, err := registry.ExpireNodeLeases(ctx, node.NodeType, node.NodeGroup, 1); err != nil || count != 0 {
		t.Fatalf("repeated ExpireNodeLeases = %d, %v", count, err)
	}
	if events := publisher.Events(); len(events) != len(beforeEvents) {
		t.Fatalf("events after repeated expiry = %+v, want unchanged %+v", events, beforeEvents)
	}
}

func TestNodeRegistryDrainRequiresInvalidMarkForExpiredOfflineTombstone(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(675, 0))
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, time.Second)
	node := testNode("game-1", "session-a")
	if _, err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if count, err := registry.ExpireNodeLeases(ctx, node.NodeType, node.NodeGroup, 1); err != nil || count != 1 {
		t.Fatalf("ExpireNodeLeases = %d, %v", count, err)
	}

	beforeNode, _ := registry.Node(node.NodeIdentity)
	beforeEvents := publisher.Events()
	if err := registry.DrainNode(ctx, node.NodeIdentity); !errors.Is(err, sp.ErrNodeNotInvalid) {
		t.Fatalf("DrainNode err = %v", err)
	}
	afterNode, ok := registry.Node(node.NodeIdentity)
	if !ok || afterNode != beforeNode || len(publisher.Events()) != len(beforeEvents) {
		t.Fatalf("DrainNode changed unmarked tombstone: before=%+v after=%+v ok=%v events=%+v", beforeNode, afterNode, ok, publisher.Events())
	}
}

func TestNodeRegistryRenewedLeaseIsNotExpiredByOldDeadline(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(700, 0))
	registry := newTestRegistry(t, clock, nil, 10*time.Second)
	node := testNode("game-1", "session-a")
	if _, err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	if _, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if count, err := registry.ExpireNodeLeases(ctx, "game", "default", 10); err != nil || count != 0 {
		t.Fatalf("ExpireNodeLeases after renewal = %d, %v", count, err)
	}
	got, _ := registry.Node(node.NodeIdentity)
	if got.Status != sp.NodeStatusActive {
		t.Fatalf("renewed node status = %s", got.Status)
	}
}
