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

func TestNodeRegistryLeaseConfig(t *testing.T) {
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

func TestNodeRegistryRegisterLeaseRules(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(100, 0)
	clock := newFakeClock(start)
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, 10*time.Second)
	node := testNode("game-1", "session-a")
	node.Status = sp.NodeStatusDraining

	if err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	got, ok := registry.Node(node.NodeIdentity)
	if !ok || got.Status != sp.NodeStatusActive || got.Lease.Version != 1 || got.Lease.TTLMillis != 10_000 || got.Lease.ExpireAtUnixMilli != start.Add(10*time.Second).UnixMilli() {
		t.Fatalf("registered node = %+v, ok=%v", got, ok)
	}

	clock.Advance(time.Second)
	if err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatalf("idempotent RegisterNode error: %v", err)
	}
	idempotent, _ := registry.Node(node.NodeIdentity)
	if idempotent.Lease != got.Lease || len(publisher.Events()) != 1 {
		t.Fatalf("idempotent register changed lease/events: lease=%+v events=%d", idempotent.Lease, len(publisher.Events()))
	}

	otherSession := node
	otherSession.NodeSessionID = "session-b"
	if err := registry.RegisterNode(ctx, otherSession); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("different-session RegisterNode err = %v", err)
	}

	registry.mu.Lock()
	offline := registry.nodes[node.NodeIdentity]
	offline.Status = sp.NodeStatusOffline
	registry.nodes[node.NodeIdentity] = offline
	registry.mu.Unlock()
	if err := registry.RegisterNode(ctx, node); !errors.Is(err, sp.ErrNodeLeaseExpired) {
		t.Fatalf("offline RegisterNode err = %v", err)
	}

	registry.mu.Lock()
	expired := registry.nodes[node.NodeIdentity]
	expired.Status = sp.NodeStatusActive
	expired.Lease.ExpireAtUnixMilli = clock.Now().UnixMilli()
	registry.nodes[node.NodeIdentity] = expired
	registry.mu.Unlock()
	if err := registry.RegisterNode(ctx, node); !errors.Is(err, sp.ErrNodeLeaseExpired) {
		t.Fatalf("expired RegisterNode err = %v", err)
	}
}

func TestNodeRegistryRegisterIdentityValidationHasNoSideEffects(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, time.Second)
	for _, node := range []sp.Node{
		{NodeGroup: "default", NodeName: "game-1", NodeIdentity: "game/default/game-1", NodeSessionID: "s"},
		{NodeType: "game", NodeGroup: "default", NodeName: "game-1", NodeSessionID: "s"},
		{NodeType: "game", NodeGroup: "default", NodeName: "game-1", NodeIdentity: "wrong", NodeSessionID: "s"},
	} {
		if err := registry.RegisterNode(context.Background(), node); err == nil {
			t.Fatalf("RegisterNode(%+v) succeeded", node)
		}
	}
	if len(registry.nodes) != 0 || len(publisher.Events()) != 0 {
		t.Fatalf("invalid registrations changed state/events: nodes=%d events=%d", len(registry.nodes), len(publisher.Events()))
	}
}

func TestNodeRegistryRenewLeaseRules(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(200, 0)
	clock := newFakeClock(start)
	registry := newTestRegistry(t, clock, nil, 10*time.Second)
	node := testNode("game-1", "session-a")
	if err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}

	clock.Advance(2 * time.Second)
	if err := registry.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
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
	if err := registry.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatalf("draining RenewNode error: %v", err)
	}
	renewed, _ = registry.Node(node.NodeIdentity)
	if renewed.Lease.Version != 3 || renewed.Lease.ExpireAtUnixMilli != clock.Now().Add(20*time.Second).UnixMilli() {
		t.Fatalf("draining renewed lease = %+v", renewed.Lease)
	}

	if err := registry.RenewNode(ctx, node.NodeIdentity, "old-session"); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("old session err = %v", err)
	}
	registry.mu.Lock()
	offline := registry.nodes[node.NodeIdentity]
	offline.Status = sp.NodeStatusOffline
	registry.nodes[node.NodeIdentity] = offline
	registry.mu.Unlock()
	if err := registry.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeNotFound) {
		t.Fatalf("offline err = %v", err)
	}
	registry.mu.Lock()
	expired := registry.nodes[node.NodeIdentity]
	expired.Status = sp.NodeStatusActive
	expired.Lease.ExpireAtUnixMilli = clock.Now().UnixMilli()
	registry.nodes[node.NodeIdentity] = expired
	registry.mu.Unlock()
	if err := registry.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeLeaseExpired) {
		t.Fatalf("expired err = %v", err)
	}
}

func TestNodeRegistryConcurrentRenewDoesNotMoveExpiryBackward(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(300, 0))
	registry := newTestRegistry(t, clock, nil, time.Minute)
	node := testNode("game-1", "session-a")
	if err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	clock.Set(time.Unix(350, 0))
	if err := registry.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatal(err)
	}
	wantExpiry, _ := registry.Node(node.NodeIdentity)
	clock.Set(time.Unix(340, 0))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := registry.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
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

func TestNodeRegistryReplaceSessionRules(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(500, 0))
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, 15*time.Second)
	oldNode := testNode("game-1", "session-a")
	oldNode.Address = "old"
	if err := registry.RegisterNode(ctx, oldNode); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkNodeInvalid(ctx, oldNode.NodeType, oldNode.NodeGroup, oldNode.NodeName); err != nil {
		t.Fatal(err)
	}
	eventsBefore := len(publisher.Events())

	same := oldNode
	same.Address = "changed"
	if _, err := registry.ReplaceNodeSession(ctx, same); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("same-session ReplaceNodeSession err = %v", err)
	}
	unchanged, _ := registry.Node(oldNode.NodeIdentity)
	if unchanged.Address != "old" || len(publisher.Events()) != eventsBefore {
		t.Fatalf("same-session replacement changed state/events: node=%+v events=%d", unchanged, len(publisher.Events()))
	}

	bad := oldNode
	bad.NodeSessionID = "session-b"
	bad.NodeIdentity = "wrong"
	if _, err := registry.ReplaceNodeSession(ctx, bad); err == nil {
		t.Fatal("identity-mismatch ReplaceNodeSession succeeded")
	}
	unchanged, _ = registry.Node(oldNode.NodeIdentity)
	if unchanged.NodeSessionID != oldNode.NodeSessionID || len(publisher.Events()) != eventsBefore {
		t.Fatal("identity-mismatch replacement changed state/events")
	}

	clock.Advance(time.Second)
	replacement := oldNode
	replacement.NodeSessionID = "session-b"
	replacement.Status = sp.NodeStatusDraining
	returnedOld, err := registry.ReplaceNodeSession(ctx, replacement)
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
		if err := registry.RegisterNode(ctx, node); err != nil {
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

func TestNodeRegistryRenewedLeaseIsNotExpiredByOldDeadline(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.Unix(700, 0))
	registry := newTestRegistry(t, clock, nil, 10*time.Second)
	node := testNode("game-1", "session-a")
	if err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	if err := registry.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
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
