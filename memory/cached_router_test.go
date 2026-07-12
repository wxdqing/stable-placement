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

type cachedRouterDirectory struct {
	mu              sync.Mutex
	route           sp.PlacementRoute
	placement       sp.Placement
	lookupErr       error
	allocateErr     error
	lookupCalls     int
	allocateCalls   int
	lookupStarted   chan struct{}
	lookupRelease   chan struct{}
	allocateStarted chan struct{}
	allocateRelease chan struct{}
}

func (d *cachedRouterDirectory) Lookup(context.Context, sp.GrainKey) (*sp.PlacementRoute, error) {
	d.mu.Lock()
	d.lookupCalls++
	err := d.lookupErr
	route := d.route
	started, release := d.lookupStarted, d.lookupRelease
	d.mu.Unlock()
	if started != nil {
		close(started)
		<-release
	}
	if err != nil {
		return nil, err
	}
	return &route, nil
}

func (d *cachedRouterDirectory) Allocate(context.Context, sp.AllocateCommand) (*sp.Placement, error) {
	d.mu.Lock()
	d.allocateCalls++
	err := d.allocateErr
	placement := d.placement
	started, release := d.allocateStarted, d.allocateRelease
	d.mu.Unlock()
	if started != nil {
		close(started)
		<-release
	}
	if err != nil {
		return nil, err
	}
	return &placement, nil
}

func (*cachedRouterDirectory) Renew(context.Context, sp.RenewCommand) (*sp.Placement, error) {
	panic("unexpected Renew")
}
func (*cachedRouterDirectory) Release(context.Context, sp.ReleaseCommand) error {
	panic("unexpected Release")
}
func (*cachedRouterDirectory) Transfer(context.Context, sp.TransferCommand) (*sp.Placement, error) {
	panic("unexpected Transfer")
}
func (*cachedRouterDirectory) Recover(context.Context, sp.RecoverCommand) (*sp.Placement, error) {
	panic("unexpected Recover")
}
func (*cachedRouterDirectory) Exists(context.Context, sp.GrainKey) (bool, error) {
	panic("unexpected Exists")
}
func (*cachedRouterDirectory) FindByNode(context.Context, sp.FindByNodeQuery) (sp.PlacementPage, error) {
	panic("unexpected FindByNode")
}

func (d *cachedRouterDirectory) calls() (int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lookupCalls, d.allocateCalls
}

func TestCachedRouterLookupUsesCacheOnlyBeforeValidUntil(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	fresh := activeRoute(key, "game/default/fresh")
	for _, test := range []struct {
		name   string
		cached sp.PlacementRoute
		calls  int
		want   string
	}{
		{name: "valid", cached: activeRoute(key, "game/default/cached"), calls: 0, want: "game/default/cached"},
		{name: "boundary", cached: sp.PlacementRoute{GrainKey: key, NodeIdentity: "stale", Status: sp.PlacementStatusActive, ValidUntil: time.Now()}, calls: 1, want: fresh.NodeIdentity},
		{name: "expired", cached: sp.PlacementRoute{GrainKey: key, NodeIdentity: "stale", Status: sp.PlacementStatusActive, ValidUntil: time.Now().Add(-time.Second)}, calls: 1, want: fresh.NodeIdentity},
		{name: "released", cached: sp.PlacementRoute{GrainKey: key, NodeIdentity: "stale", Status: sp.PlacementStatusReleased, ValidUntil: time.Now().Add(time.Hour)}, calls: 1, want: fresh.NodeIdentity},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := &cachedRouterDirectory{route: fresh}
			cache := NewPlacementCache()
			cache.SetCachedPlacement(key, test.cached)
			route, err := NewCachedRouter(directory, cache).Lookup(context.Background(), key)
			if err != nil || route.NodeIdentity != test.want {
				t.Fatalf("Lookup = %+v, %v", route, err)
			}
			calls, _ := directory.calls()
			if calls != test.calls {
				t.Fatalf("Lookup calls = %d, want %d", calls, test.calls)
			}
		})
	}
}

func TestCachedRouterLookupCachesDirectoryRouteDirectly(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	want := activeRoute(key, "game/default/game-1")
	directory := &cachedRouterDirectory{route: want}
	router := NewCachedRouter(directory, NewPlacementCache())
	first, err := router.Lookup(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	second, err := router.Lookup(context.Background(), key)
	if err != nil || *first != want || *second != want {
		t.Fatalf("routes = %+v %+v, err=%v", first, second, err)
	}
	calls, _ := directory.calls()
	if calls != 1 {
		t.Fatalf("Lookup calls = %d", calls)
	}
}

func TestCachedRouterAllocateLooksUpRouteBeforeCaching(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	placement := activePlacement(key, "game/default/game-1")
	route := activeRoute(key, placement.NodeIdentity)
	directory := &cachedRouterDirectory{placement: placement, route: route}
	cache := NewPlacementCache()
	router := NewCachedRouter(directory, cache)

	got, err := router.Allocate(context.Background(), sp.AllocateCommand{GrainID: "10001", Kind: "Player"})
	if err != nil || *got != placement {
		t.Fatalf("Allocate = %+v, %v", got, err)
	}
	cached, ok := cache.GetCachedPlacement(key)
	lookupCalls, allocateCalls := directory.calls()
	if !ok || *cached != route || lookupCalls != 1 || allocateCalls != 1 {
		t.Fatalf("cached=%+v ok=%v calls=%d/%d", cached, ok, lookupCalls, allocateCalls)
	}
}

func TestCachedRouterAllocateReturnsPlacementWhenRouteLookupFails(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	placement := activePlacement(key, "game/default/game-1")
	wantErr := errors.New("lease changed")
	directory := &cachedRouterDirectory{placement: placement, lookupErr: wantErr}
	cache := NewPlacementCache()
	got, err := NewCachedRouter(directory, cache).Allocate(context.Background(), sp.AllocateCommand{GrainID: "10001", Kind: "Player"})
	if err != nil || *got != placement {
		t.Fatalf("Allocate = %+v, %v", got, err)
	}
	if _, ok := cache.GetCachedPlacement(key); ok {
		t.Fatal("Allocate cached without route")
	}
}

func TestCachedRouterHandleEventMappings(t *testing.T) {
	grainEvents := []sp.EventType{sp.EventPlacementCreated, sp.EventPlacementReleased, sp.EventPlacementTransferred, sp.EventPlacementRecovered, sp.EventPlacementCacheInvalidated}
	for _, eventType := range grainEvents {
		t.Run(string(eventType), func(t *testing.T) {
			router, cache, keys := seededCachedRouter(t)
			if err := router.HandleEvent(sp.PlacementEvent{Type: eventType, GrainKey: keys[0]}); err != nil {
				t.Fatal(err)
			}
			assertCached(t, cache, keys[0], false)
			assertCached(t, cache, keys[1], true)
		})
	}

	nodeEvents := []sp.EventType{sp.EventNodeLeaseExpired, sp.EventNodeReplaced, sp.EventNodeDraining, sp.EventNodeMarkedInvalid, sp.EventNodeUnregistered}
	for _, eventType := range nodeEvents {
		t.Run(string(eventType), func(t *testing.T) {
			router, cache, keys := seededCachedRouter(t)
			if err := router.HandleEvent(sp.PlacementEvent{Type: eventType, NodeIdentity: "game/default/game-1"}); err != nil {
				t.Fatal(err)
			}
			assertCached(t, cache, keys[0], false)
			assertCached(t, cache, keys[1], true)
		})
	}

	t.Run("renew audit ignored", func(t *testing.T) {
		router, cache, keys := seededCachedRouter(t)
		before := router.epoch
		if err := router.HandleEvent(sp.PlacementEvent{Type: sp.EventPlacementRenewed}); err != nil {
			t.Fatal(err)
		}
		if router.epoch != before {
			t.Fatalf("epoch advanced from %d to %d", before, router.epoch)
		}
		for _, key := range keys {
			assertCached(t, cache, key, true)
		}
	})

	t.Run("restored group", func(t *testing.T) {
		router, cache, keys := seededCachedRouter(t)
		_ = router.HandleEvent(sp.PlacementEvent{Type: sp.EventNodeRestored, NodeType: "game", NodeGroup: "default"})
		assertCached(t, cache, keys[0], false)
		assertCached(t, cache, keys[1], true)
	})

	for _, event := range []sp.PlacementEvent{
		{Type: sp.EventNodeLeaseExpired},
		{Type: sp.EventPlacementCreated},
		{Type: sp.EventNodeRestored, NodeType: "game"},
		{Type: sp.EventType("FutureUnknown")},
	} {
		t.Run("conservative/"+string(event.Type), func(t *testing.T) {
			router, cache, keys := seededCachedRouter(t)
			_ = router.HandleEvent(event)
			for _, key := range keys {
				assertCached(t, cache, key, false)
			}
		})
	}
}

func TestCachedRouterEventBusIncompleteNodeLeaseExpiredClearsAll(t *testing.T) {
	router, cache, keys := seededCachedRouter(t)
	bus := NewEventBus()
	if err := bus.Subscribe(context.Background(), router.HandleEvent); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), sp.PlacementEvent{Type: sp.EventNodeLeaseExpired}); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		assertCached(t, cache, key, false)
	}
}

func TestCachedRouterInflightLookupCannotRefillAcrossEventOrHealthChange(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	for _, transition := range []string{"event", "degrade", "recover"} {
		t.Run(transition, func(t *testing.T) {
			directory := &cachedRouterDirectory{route: activeRoute(key, "game/default/game-1"), lookupStarted: make(chan struct{}), lookupRelease: make(chan struct{})}
			cache := NewPlacementCache()
			router := NewCachedRouter(directory, cache)
			if transition == "recover" {
				router.Degrade()
			}
			done := make(chan error, 1)
			go func() { _, err := router.Lookup(context.Background(), key); done <- err }()
			<-directory.lookupStarted
			switch transition {
			case "event":
				_ = router.HandleEvent(sp.PlacementEvent{Type: sp.EventNodeLeaseExpired, NodeIdentity: "game/default/game-1"})
			case "degrade":
				router.Degrade()
			case "recover":
				router.Recover()
			}
			close(directory.lookupRelease)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			assertCached(t, cache, key, false)
		})
	}
}

func TestCachedRouterInflightAllocateCannotRefillAcrossInvalidation(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	for _, stage := range []string{"allocate", "lookup"} {
		t.Run(stage, func(t *testing.T) {
			directory := &cachedRouterDirectory{
				placement: activePlacement(key, "game/default/game-1"),
				route:     activeRoute(key, "game/default/game-1"),
			}
			if stage == "allocate" {
				directory.allocateStarted = make(chan struct{})
				directory.allocateRelease = make(chan struct{})
			} else {
				directory.lookupStarted = make(chan struct{})
				directory.lookupRelease = make(chan struct{})
			}
			cache := NewPlacementCache()
			router := NewCachedRouter(directory, cache)
			done := make(chan error, 1)
			go func() {
				_, err := router.Allocate(context.Background(), sp.AllocateCommand{GrainID: "10001", Kind: "Player"})
				done <- err
			}()
			if stage == "allocate" {
				<-directory.allocateStarted
			} else {
				<-directory.lookupStarted
			}
			if err := router.HandleEvent(sp.PlacementEvent{Type: sp.EventNodeLeaseExpired, NodeIdentity: "game/default/game-1"}); err != nil {
				t.Fatal(err)
			}
			if stage == "allocate" {
				close(directory.allocateRelease)
			} else {
				close(directory.lookupRelease)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			assertCached(t, cache, key, false)
		})
	}
}

func TestCachedRouterAllocateCachesAfterSynchronousPlacementCreated(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus()
	registry, err := NewNodeRegistry(bus, sp.DefaultNodeLeaseConfig())
	if err != nil {
		t.Fatal(err)
	}
	directory, err := NewDirectory(registry, sp.StrategyModeGo, strategies.NewRoundRobin(), bus)
	if err != nil {
		t.Fatal(err)
	}
	cache := NewPlacementCache()
	router := NewCachedRouter(directory, cache)
	if err := bus.Subscribe(ctx, router.HandleEvent); err != nil {
		t.Fatal(err)
	}
	node := testNode("game-1", "session-a")
	if err := registry.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	placement, err := router.Allocate(ctx, sp.AllocateCommand{GrainID: "10001", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := cache.GetCachedPlacement(placement.GrainKey)
	if !ok || route.Version != placement.Version || route.OwnerNodeSessionID != node.NodeSessionID {
		t.Fatalf("cached route = %+v, ok=%v", route, ok)
	}
}

func TestCachedRouterDegradeBypassesAndRecoverClearsCache(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	directory := &cachedRouterDirectory{route: activeRoute(key, "fresh")}
	cache := NewPlacementCache()
	cache.SetCachedPlacement(key, activeRoute(key, "stale"))
	router := NewCachedRouter(directory, cache)
	router.Degrade()
	if route, err := router.Lookup(context.Background(), key); err != nil || route.NodeIdentity != "fresh" {
		t.Fatalf("degraded Lookup = %+v, %v", route, err)
	}
	if _, ok := cache.GetCachedPlacement(key); ok {
		t.Fatal("degraded lookup refilled cache")
	}
	router.Recover()
	if cache.IsDegraded() {
		t.Fatal("cache remained degraded")
	}
}

func activePlacement(key sp.GrainKey, nodeIdentity string) sp.Placement {
	return sp.Placement{GrainID: "10001", Kind: "Player", GrainKey: key, NodeIdentity: nodeIdentity, OwnerNodeSessionID: "session-a", Version: 3, Status: sp.PlacementStatusActive}
}

func activeRoute(key sp.GrainKey, nodeIdentity string) sp.PlacementRoute {
	return sp.PlacementRoute{GrainKey: key, NodeIdentity: nodeIdentity, OwnerNodeSessionID: "session-a", Version: 3, Status: sp.PlacementStatusActive, NodeLeaseVersion: 2, ValidUntil: time.Now().Add(time.Hour)}
}

func seededCachedRouter(t *testing.T) (*CachedRouter, *PlacementCache, []sp.GrainKey) {
	t.Helper()
	cache := NewPlacementCache()
	keys := make([]sp.GrainKey, 2)
	keys[0], _ = sp.NewGrainKey("Player", "1")
	keys[1], _ = sp.NewGrainKey("Player", "2")
	cache.SetCachedPlacement(keys[0], activeRoute(keys[0], "game/default/game-1"))
	cache.SetCachedPlacement(keys[1], activeRoute(keys[1], "game/other/game-2"))
	return NewCachedRouter(&cachedRouterDirectory{}, cache), cache, keys
}

func assertCached(t *testing.T, cache *PlacementCache, key sp.GrainKey, want bool) {
	t.Helper()
	_, got := cache.GetCachedPlacement(key)
	if got != want {
		t.Fatalf("cache key %q present = %v, want %v", key, got, want)
	}
}

func TestCachedRouterConcurrentHealthTransitionsAndLookup(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	directory := &cachedRouterDirectory{route: activeRoute(key, "game/default/game-1")}
	router := NewCachedRouter(directory, NewPlacementCache())
	var wg sync.WaitGroup
	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				switch (worker + i) % 3 {
				case 0:
					router.Degrade()
				case 1:
					router.Recover()
				default:
					if _, err := router.Lookup(context.Background(), key); err != nil {
						t.Errorf("Lookup: %v", err)
					}
				}
			}
		}(worker)
	}
	wg.Wait()
}
