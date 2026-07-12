package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/strategies"
)

type cachedRouterDirectory struct {
	mu            sync.Mutex
	placement     sp.Placement
	lookupErr     error
	allocateErr   error
	lookupCalls   int
	allocateCalls int
}

func (d *cachedRouterDirectory) Lookup(context.Context, sp.GrainKey) (*sp.Placement, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lookupCalls++
	if d.lookupErr != nil {
		return nil, d.lookupErr
	}
	placement := d.placement
	return &placement, nil
}

func (d *cachedRouterDirectory) Allocate(context.Context, sp.AllocateCommand) (*sp.Placement, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.allocateCalls++
	if d.allocateErr != nil {
		return nil, d.allocateErr
	}
	placement := d.placement
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

func (*cachedRouterDirectory) Expire(context.Context, sp.ExpireCommand) error {
	panic("unexpected Expire")
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

func TestCachedRouterLookupCachesOnlyUsableRoutes(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	directory := &cachedRouterDirectory{placement: activePlacement(key, "game/default/game-1")}
	cache := NewPlacementCache()
	router := NewCachedRouter(directory, cache)

	first, err := router.Lookup(context.Background(), key)
	if err != nil {
		t.Fatalf("first Lookup error: %v", err)
	}
	second, err := router.Lookup(context.Background(), key)
	if err != nil {
		t.Fatalf("second Lookup error: %v", err)
	}
	if first.NodeIdentity != directory.placement.NodeIdentity || *first != *second {
		t.Fatalf("routes = first %+v, second %+v", first, second)
	}
	lookupCalls, _ := directory.calls()
	if lookupCalls != 1 {
		t.Fatalf("Directory Lookup calls = %d, want 1", lookupCalls)
	}

	for _, test := range []struct {
		name  string
		route sp.PlacementRoute
	}{
		{name: "non-active", route: sp.PlacementRoute{GrainKey: key, NodeIdentity: "stale", Status: sp.PlacementStatusReleased, LeaseExpireAt: time.Now().Add(time.Hour)}},
		{name: "expired", route: sp.PlacementRoute{GrainKey: key, NodeIdentity: "stale", Status: sp.PlacementStatusActive, LeaseExpireAt: time.Now().Add(-time.Second)}},
		{name: "missing expiry", route: sp.PlacementRoute{GrainKey: key, NodeIdentity: "stale", Status: sp.PlacementStatusActive}},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := &cachedRouterDirectory{placement: activePlacement(key, "game/default/game-2")}
			cache := NewPlacementCache()
			cache.SetCachedPlacement(key, test.route)
			route, err := NewCachedRouter(directory, cache).Lookup(context.Background(), key)
			if err != nil {
				t.Fatalf("Lookup error: %v", err)
			}
			if route.NodeIdentity != "game/default/game-2" {
				t.Fatalf("Lookup route = %+v, want fresh Directory route", route)
			}
			lookupCalls, _ := directory.calls()
			if lookupCalls != 1 {
				t.Fatalf("Directory Lookup calls = %d, want 1", lookupCalls)
			}
		})
	}
}

func TestCachedRouterAllocateCachesPlacementAndPropagatesDirectoryErrors(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	directory := &cachedRouterDirectory{placement: activePlacement(key, "game/default/game-1")}
	cache := NewPlacementCache()
	router := NewCachedRouter(directory, cache)

	placement, err := router.Allocate(context.Background(), sp.AllocateCommand{GrainID: "10001", Kind: "Player"})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	route, ok := cache.GetCachedPlacement(key)
	if !ok || route.NodeIdentity != placement.NodeIdentity || route.Version != placement.Version {
		t.Fatalf("cached route = %+v, ok %v; placement = %+v", route, ok, placement)
	}

	wantErr := errors.New("directory unavailable")
	directory.lookupErr = wantErr
	cache.DeleteCachedPlacement(key)
	if _, err := router.Lookup(context.Background(), key); !errors.Is(err, wantErr) {
		t.Fatalf("Lookup err = %v, want %v", err, wantErr)
	}
	directory.allocateErr = wantErr
	if _, err := router.Allocate(context.Background(), sp.AllocateCommand{}); !errors.Is(err, wantErr) {
		t.Fatalf("Allocate err = %v, want %v", err, wantErr)
	}
}

func TestCachedRouterHandleEventMappings(t *testing.T) {
	grainEvents := []sp.EventType{
		sp.EventPlacementCreated,
		sp.EventPlacementRenewed,
		sp.EventPlacementReleased,
		sp.EventPlacementTransferred,
		sp.EventPlacementRecovered,
		sp.EventLeaseExpired,
		sp.EventPlacementCacheInvalidated,
	}
	for _, eventType := range grainEvents {
		t.Run(string(eventType), func(t *testing.T) {
			router, cache, keys := seededCachedRouter(t)
			if err := router.HandleEvent(sp.PlacementEvent{Type: eventType, GrainKey: keys[0]}); err != nil {
				t.Fatalf("HandleEvent error: %v", err)
			}
			assertCached(t, cache, keys[0], false)
			assertCached(t, cache, keys[1], true)
			assertCached(t, cache, keys[2], true)
		})
	}

	nodeEvents := []sp.EventType{
		sp.EventNodeReplaced,
		sp.EventNodeDraining,
		sp.EventNodeMarkedInvalid,
		sp.EventNodeUnregistered,
	}
	for _, eventType := range nodeEvents {
		t.Run(string(eventType), func(t *testing.T) {
			router, cache, keys := seededCachedRouter(t)
			if err := router.HandleEvent(sp.PlacementEvent{Type: eventType, NodeIdentity: "game/default/game-1"}); err != nil {
				t.Fatalf("HandleEvent error: %v", err)
			}
			assertCached(t, cache, keys[0], false)
			assertCached(t, cache, keys[1], true)
			assertCached(t, cache, keys[2], true)
		})
	}

	t.Run("NodeRestored", func(t *testing.T) {
		router, cache, keys := seededCachedRouter(t)
		if err := router.HandleEvent(sp.PlacementEvent{Type: sp.EventNodeRestored, NodeType: "game", NodeGroup: "default"}); err != nil {
			t.Fatalf("HandleEvent error: %v", err)
		}
		assertCached(t, cache, keys[0], false)
		assertCached(t, cache, keys[1], true)
		assertCached(t, cache, keys[2], false)
	})

	for _, eventType := range []sp.EventType{sp.EventManualCacheClear, sp.EventType("FutureUnknownEvent")} {
		t.Run(string(eventType), func(t *testing.T) {
			router, cache, keys := seededCachedRouter(t)
			if err := router.HandleEvent(sp.PlacementEvent{Type: eventType}); err != nil {
				t.Fatalf("HandleEvent error: %v", err)
			}
			for _, key := range keys {
				assertCached(t, cache, key, false)
			}
		})
	}

	t.Run("NodeRegisteredDoesNotInvalidate", func(t *testing.T) {
		router, cache, keys := seededCachedRouter(t)
		if err := router.HandleEvent(sp.PlacementEvent{Type: sp.EventNodeRegistered}); err != nil {
			t.Fatalf("HandleEvent error: %v", err)
		}
		for _, key := range keys {
			assertCached(t, cache, key, true)
		}
	})
}

func TestCachedRouterDegradeBypassesCacheAndRecoverClearsBeforeEnabling(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	directory := &cachedRouterDirectory{placement: activePlacement(key, "game/default/game-2")}
	cache := NewPlacementCache()
	cache.SetCachedPlacement(key, routeFromPlacement(activePlacement(key, "game/default/stale")))
	router := NewCachedRouter(directory, cache)

	router.Degrade()
	for i := 0; i < 2; i++ {
		route, err := router.Lookup(context.Background(), key)
		if err != nil {
			t.Fatalf("degraded Lookup %d error: %v", i, err)
		}
		if route.NodeIdentity != "game/default/game-2" {
			t.Fatalf("degraded Lookup %d route = %+v", i, route)
		}
	}
	lookupCalls, _ := directory.calls()
	if lookupCalls != 2 {
		t.Fatalf("degraded Directory Lookup calls = %d, want 2", lookupCalls)
	}
	cache.mu.RLock()
	entries := len(cache.entries)
	cache.mu.RUnlock()
	if entries != 0 {
		t.Fatalf("degraded cache entries = %d, want 0", entries)
	}

	cache.mu.Lock()
	cache.entries[key] = routeFromPlacement(directory.placement)
	cache.byNode[directory.placement.NodeIdentity] = map[sp.GrainKey]struct{}{key: {}}
	cache.mu.Unlock()
	router.Recover()
	if cache.IsDegraded() {
		t.Fatal("cache remained degraded after Recover")
	}
	assertCached(t, cache, key, false)
	if _, err := router.Lookup(context.Background(), key); err != nil {
		t.Fatalf("healthy Lookup error: %v", err)
	}
	if _, err := router.Lookup(context.Background(), key); err != nil {
		t.Fatalf("cached healthy Lookup error: %v", err)
	}
	lookupCalls, _ = directory.calls()
	if lookupCalls != 3 {
		t.Fatalf("Directory Lookup calls after recovery = %d, want 3", lookupCalls)
	}

	ordered := &orderedPlacementCache{PlacementCache: NewPlacementCache()}
	NewCachedRouter(directory, ordered).Recover()
	if fmt.Sprint(ordered.calls) != "[clear degraded:false]" {
		t.Fatalf("Recover cache calls = %v, want clear before degraded:false", ordered.calls)
	}
}

func TestCachedRouterConcurrentDegradeRecoverAndLookup(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	directory := &cachedRouterDirectory{placement: activePlacement(key, "game/default/game-1")}
	router := NewCachedRouter(directory, NewPlacementCache())

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				switch (worker + i) % 3 {
				case 0:
					router.Degrade()
				case 1:
					router.Recover()
				default:
					if _, err := router.Lookup(context.Background(), key); err != nil {
						t.Errorf("Lookup error: %v", err)
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
}

func TestCachedRouterLookupDoesNotRefillAfterInvalidation(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	events := []sp.PlacementEvent{
		{Type: sp.EventPlacementReleased, GrainKey: key, PlacementVersion: 4},
		{Type: sp.EventPlacementTransferred, GrainKey: key, PlacementVersion: 4},
		{Type: sp.EventLeaseExpired, GrainKey: key, PlacementVersion: 4},
		{Type: sp.EventNodeMarkedInvalid, NodeIdentity: "game/default/game-1"},
	}
	for _, event := range events {
		t.Run(string(event.Type), func(t *testing.T) {
			directory := newBlockingCachedRouterDirectory(activePlacement(key, "game/default/game-1"))
			cache := NewPlacementCache()
			router := NewCachedRouter(directory, cache)

			result := make(chan error, 1)
			go func() {
				_, err := router.Lookup(context.Background(), key)
				result <- err
			}()
			<-directory.lookupStarted
			if err := router.HandleEvent(event); err != nil {
				t.Fatalf("HandleEvent error: %v", err)
			}
			close(directory.lookupRelease)
			if err := <-result; err != nil {
				t.Fatalf("Lookup error: %v", err)
			}
			assertCached(t, cache, key, false)
		})
	}
}

func TestCachedRouterAllocateDoesNotRefillAfterInvalidation(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	directory := newBlockingCachedRouterDirectory(activePlacement(key, "game/default/game-1"))
	cache := NewPlacementCache()
	router := NewCachedRouter(directory, cache)

	result := make(chan error, 1)
	go func() {
		_, err := router.Allocate(context.Background(), sp.AllocateCommand{Kind: "Player", GrainID: "10001"})
		result <- err
	}()
	<-directory.allocateStarted
	if err := router.HandleEvent(sp.PlacementEvent{
		Type:             sp.EventPlacementTransferred,
		GrainKey:         key,
		PlacementVersion: 4,
	}); err != nil {
		t.Fatalf("HandleEvent error: %v", err)
	}
	close(directory.allocateRelease)
	if err := <-result; err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	assertCached(t, cache, key, false)
}

func TestCachedRouterAllocateDoesNotRefillOlderThanCreatedEvent(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	directory := newBlockingCachedRouterDirectory(activePlacement(key, "game/default/game-1"))
	cache := NewPlacementCache()
	router := NewCachedRouter(directory, cache)

	result := make(chan error, 1)
	go func() {
		_, err := router.Allocate(context.Background(), sp.AllocateCommand{Kind: "Player", GrainID: "10001"})
		result <- err
	}()
	<-directory.allocateStarted
	if err := router.HandleEvent(sp.PlacementEvent{
		Type:             sp.EventPlacementCreated,
		GrainKey:         key,
		PlacementVersion: directory.placement.Version + 1,
	}); err != nil {
		t.Fatalf("HandleEvent error: %v", err)
	}
	close(directory.allocateRelease)
	if err := <-result; err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	assertCached(t, cache, key, false)
}

func TestCachedRouterInflightRefillCannotCrossHealthTransition(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	for _, operation := range []string{"lookup", "allocate"} {
		for _, transition := range []string{"degrade", "recover"} {
			t.Run(operation+"/"+transition, func(t *testing.T) {
				directory := newBlockingCachedRouterDirectory(activePlacement(key, "game/default/game-1"))
				cache := NewPlacementCache()
				router := NewCachedRouter(directory, cache)
				if transition == "recover" {
					router.Degrade()
				}

				result := make(chan error, 1)
				go func() {
					var err error
					if operation == "lookup" {
						_, err = router.Lookup(context.Background(), key)
					} else {
						_, err = router.Allocate(context.Background(), sp.AllocateCommand{Kind: "Player", GrainID: "10001"})
					}
					result <- err
				}()
				if operation == "lookup" {
					<-directory.lookupStarted
				} else {
					<-directory.allocateStarted
				}
				if transition == "degrade" {
					router.Degrade()
				} else {
					router.Recover()
				}
				if operation == "lookup" {
					close(directory.lookupRelease)
				} else {
					close(directory.allocateRelease)
				}
				if err := <-result; err != nil {
					t.Fatalf("%s error: %v", operation, err)
				}
				assertCached(t, cache, key, false)
			})
		}
	}
}

func TestCachedRouterAllocateCachesAfterSynchronousPlacementCreated(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus()
	directory, err := NewDirectory(NewNodeRegistry(bus), sp.StrategyModeGo, strategies.NewRoundRobin(), bus)
	if err != nil {
		t.Fatalf("NewDirectory error: %v", err)
	}
	cache := NewPlacementCache()
	router := NewCachedRouter(directory, cache)
	if err := bus.Subscribe(ctx, router.HandleEvent); err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	nodeIdentity, _ := sp.NewNodeIdentity("game", "default", "game-1")
	if err := directory.NodeRegistry().RegisterNode(ctx, sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      "game-1",
		NodeIdentity:  nodeIdentity.String(),
		NodeSessionID: "session-a",
		Status:        sp.NodeStatusActive,
	}); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}

	placement, err := router.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	route, ok := cache.GetCachedPlacement(placement.GrainKey)
	if !ok || route.Version != placement.Version {
		t.Fatalf("cached route = %+v, ok %v; placement = %+v", route, ok, placement)
	}
}

func activePlacement(key sp.GrainKey, nodeIdentity string) sp.Placement {
	return sp.Placement{
		GrainKey:      key,
		NodeIdentity:  nodeIdentity,
		Version:       3,
		Status:        sp.PlacementStatusActive,
		LeaseExpireAt: time.Now().Add(time.Hour),
	}
}

func routeFromPlacement(placement sp.Placement) sp.PlacementRoute {
	return sp.PlacementRoute{
		GrainKey:      placement.GrainKey,
		NodeIdentity:  placement.NodeIdentity,
		Version:       placement.Version,
		Status:        placement.Status,
		LeaseExpireAt: placement.LeaseExpireAt,
	}
}

func seededCachedRouter(t *testing.T) (*CachedRouter, *PlacementCache, []sp.GrainKey) {
	t.Helper()
	cache := NewPlacementCache()
	keys := make([]sp.GrainKey, 3)
	for i := range keys {
		keys[i], _ = sp.NewGrainKey("Player", fmt.Sprint(i+1))
	}
	cache.SetCachedPlacement(keys[0], routeFromPlacement(activePlacement(keys[0], "game/default/game-1")))
	cache.SetCachedPlacement(keys[1], routeFromPlacement(activePlacement(keys[1], "game/other/game-2")))
	cache.SetCachedPlacement(keys[2], routeFromPlacement(activePlacement(keys[2], "game/default/game-3")))
	return NewCachedRouter(&cachedRouterDirectory{}, cache), cache, keys
}

func assertCached(t *testing.T, cache *PlacementCache, key sp.GrainKey, want bool) {
	t.Helper()
	_, got := cache.GetCachedPlacement(key)
	if got != want {
		t.Fatalf("cache key %q present = %v, want %v", key, got, want)
	}
}

type orderedPlacementCache struct {
	*PlacementCache
	calls []string
}

type blockingCachedRouterDirectory struct {
	*cachedRouterDirectory
	lookupStarted   chan struct{}
	lookupRelease   chan struct{}
	allocateStarted chan struct{}
	allocateRelease chan struct{}
}

func newBlockingCachedRouterDirectory(placement sp.Placement) *blockingCachedRouterDirectory {
	return &blockingCachedRouterDirectory{
		cachedRouterDirectory: &cachedRouterDirectory{placement: placement},
		lookupStarted:         make(chan struct{}),
		lookupRelease:         make(chan struct{}),
		allocateStarted:       make(chan struct{}),
		allocateRelease:       make(chan struct{}),
	}
}

func (d *blockingCachedRouterDirectory) Lookup(context.Context, sp.GrainKey) (*sp.Placement, error) {
	placement := d.placement
	close(d.lookupStarted)
	<-d.lookupRelease
	return &placement, nil
}

func (d *blockingCachedRouterDirectory) Allocate(context.Context, sp.AllocateCommand) (*sp.Placement, error) {
	placement := d.placement
	close(d.allocateStarted)
	<-d.allocateRelease
	return &placement, nil
}

func (c *orderedPlacementCache) ClearPlacementCache() {
	c.calls = append(c.calls, "clear")
	c.PlacementCache.ClearPlacementCache()
}

func (c *orderedPlacementCache) SetDegraded(degraded bool) {
	c.calls = append(c.calls, fmt.Sprintf("degraded:%v", degraded))
	c.PlacementCache.SetDegraded(degraded)
}
