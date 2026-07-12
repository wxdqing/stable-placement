package protoactor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/service/cluster"
	sp "github.com/wxdqing/stable-placement"
)

type resolverDirectory struct {
	mu    sync.Mutex
	route sp.PlacementRoute
	calls int
}

func (d *resolverDirectory) ResolveRoute(context.Context, sp.ResolveRouteCommand) (*sp.PlacementRoute, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	route := d.route
	return &route, nil
}

func (d *resolverDirectory) Lookup(context.Context, sp.GrainKey) (*sp.PlacementRoute, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	route := d.route
	return &route, nil
}

type recordingActivator struct {
	mu    sync.Mutex
	calls int
	pid   *actor.PID
}

func (a *recordingActivator) Activate(context.Context, *cluster.ClusterIdentity, sp.PlacementRoute) (*actor.PID, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return a.pid, nil
}

func TestResolverCachesOnlyUntilRouteValidUntil(t *testing.T) {
	now := time.Unix(1_000, 0)
	directory := &resolverDirectory{route: sp.PlacementRoute{
		GrainKey: "player/acct-1", NodeIdentity: "game/server-1/game-1", OwnerNodeSessionID: "session-a",
		Version: 1, NodeLeaseVersion: 1, Status: sp.PlacementStatusActive, ValidUntil: now.Add(time.Second),
	}}
	activator := &recordingActivator{pid: actor.NewPID("127.0.0.1:8000", "player-acct-1")}
	resolver := NewResolver(directory, map[string]sp.KindRouteConfig{
		"player": {NodeType: "game", NodeGroupPrefix: "server-", GroupIDLabel: "server_id"},
	}, activator)
	resolver.now = func() time.Time { return now }
	identity := cluster.NewClusterIdentity("acct-1", "player")
	placement := &cluster.PlacementContext{Labels: map[string]string{"server_id": "1"}}

	first, err := resolver.ResolvePID(context.Background(), placement, identity)
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolver.ResolvePID(context.Background(), placement, identity)
	if err != nil || first.PID != second.PID {
		t.Fatalf("routes=%+v %+v err=%v", first, second, err)
	}
	if directory.calls != 1 || activator.calls != 1 {
		t.Fatalf("calls directory=%d activator=%d", directory.calls, activator.calls)
	}

	now = now.Add(time.Second)
	directory.route.ValidUntil = now.Add(time.Second)
	refreshed, err := resolver.ResolvePID(context.Background(), placement, identity)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.PID != first.PID || directory.calls != 2 || activator.calls != 1 || !refreshed.ValidUntil.Equal(directory.route.ValidUntil) {
		t.Fatalf("refreshed=%+v calls=%d/%d", refreshed, directory.calls, activator.calls)
	}
}

func TestResolverDegradeClearsAndDisablesCache(t *testing.T) {
	now := time.Unix(1_000, 0)
	directory := &resolverDirectory{route: sp.PlacementRoute{GrainKey: "player/acct-1", NodeIdentity: "game/server-1/game-1", OwnerNodeSessionID: "s", Version: 1, NodeLeaseVersion: 1, Status: sp.PlacementStatusActive, ValidUntil: now.Add(time.Minute)}}
	activator := &recordingActivator{pid: actor.NewPID("local", "p")}
	resolver := NewResolver(directory, map[string]sp.KindRouteConfig{"player": {NodeType: "game", NodeGroupPrefix: "server-", GroupIDLabel: "server_id"}}, activator)
	resolver.now = func() time.Time { return now }
	pc := &cluster.PlacementContext{Labels: map[string]string{"server_id": "1"}}
	ci := cluster.NewClusterIdentity("acct-1", "player")
	if _, err := resolver.ResolvePID(context.Background(), pc, ci); err != nil {
		t.Fatal(err)
	}
	resolver.Degrade()
	if _, err := resolver.ResolvePID(context.Background(), pc, ci); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolvePID(context.Background(), pc, ci); err != nil {
		t.Fatal(err)
	}
	if directory.calls != 3 || activator.calls != 3 {
		t.Fatalf("calls=%d/%d", directory.calls, activator.calls)
	}
}

func TestResolverRemoveClearsActivatorPIDWithoutReleasingPlacement(t *testing.T) {
	pid := actor.NewPID("local", "player")
	activator := &removableActivator{recordingActivator: recordingActivator{pid: pid}}
	resolver := NewResolver(&resolverDirectory{}, map[string]sp.KindRouteConfig{}, activator)
	identity := cluster.NewClusterIdentity("acct-1", "player")
	resolver.cache["player/acct-1"] = PIDRoute{PID: pid, GrainKey: "player/acct-1"}
	resolver.Remove(identity, pid)
	if activator.removeCalls != 1 {
		t.Fatalf("activator remove calls=%d", activator.removeCalls)
	}
	if _, ok := resolver.cache["player/acct-1"]; ok {
		t.Fatal("resolver cache retained stopped PID")
	}
}

func TestResolverFenceClearsCacheAndRejectsFutureResolution(t *testing.T) {
	resolver := NewResolver(&resolverDirectory{}, map[string]sp.KindRouteConfig{
		"player": {NodeType: "game", NodeGroupPrefix: "server-", GroupIDLabel: "server_id"},
	}, &recordingActivator{pid: actor.NewPID("local", "player")})
	resolver.cache["player/acct-1"] = PIDRoute{PID: actor.NewPID("local", "player")}

	resolver.Fence()

	if len(resolver.cache) != 0 {
		t.Fatal("Fence retained PID route cache")
	}
	_, err := resolver.ResolvePID(context.Background(), &cluster.PlacementContext{
		Labels: map[string]string{"server_id": "1"},
	}, cluster.NewClusterIdentity("acct-1", "player"))
	if !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("ResolvePID after Fence error = %v", err)
	}
}

func TestResolverFenceWaitsForInFlightResolutionThenClearsItsRoute(t *testing.T) {
	now := time.Now()
	directory := &blockingResolverDirectory{
		entered: make(chan struct{}), release: make(chan struct{}),
		route: sp.PlacementRoute{
			GrainKey: "player/acct-1", NodeIdentity: "game/server-1/game-1", OwnerNodeSessionID: "session-1",
			Version: 1, ValidUntil: now.Add(time.Minute),
		},
	}
	resolver := NewResolver(directory, map[string]sp.KindRouteConfig{
		"player": {NodeType: "game", NodeGroupPrefix: "server-", GroupIDLabel: "server_id"},
	}, &recordingActivator{pid: actor.NewPID("local", "player")})
	resolved := make(chan error, 1)
	go func() {
		_, err := resolver.ResolvePID(context.Background(), &cluster.PlacementContext{Labels: map[string]string{"server_id": "1"}}, cluster.NewClusterIdentity("acct-1", "player"))
		resolved <- err
	}()
	<-directory.entered
	fenced := make(chan struct{})
	go func() { resolver.Fence(); close(fenced) }()
	select {
	case <-fenced:
		t.Fatal("Fence completed while a resolution was in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(directory.release)
	if err := <-resolved; err != nil {
		t.Fatal(err)
	}
	<-fenced
	if len(resolver.cache) != 0 {
		t.Fatal("Fence retained route created by in-flight resolution")
	}
}

type blockingResolverDirectory struct {
	entered chan struct{}
	release chan struct{}
	route   sp.PlacementRoute
}

func (d *blockingResolverDirectory) ResolveRoute(context.Context, sp.ResolveRouteCommand) (*sp.PlacementRoute, error) {
	close(d.entered)
	<-d.release
	route := d.route
	return &route, nil
}

func (d *blockingResolverDirectory) Lookup(context.Context, sp.GrainKey) (*sp.PlacementRoute, error) {
	route := d.route
	return &route, nil
}

type removableActivator struct {
	recordingActivator
	removeCalls int
}

func (a *removableActivator) Remove(*cluster.ClusterIdentity, *actor.PID) { a.removeCalls++ }
