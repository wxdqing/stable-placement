package memory

import (
	"context"
	"sync"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

type CachedRouter struct {
	directory sp.Directory
	cache     sp.PlacementCache

	mu             sync.Mutex
	epoch          uint64
	lastEvent      sp.PlacementEvent
	lastEventEpoch uint64
}

func NewCachedRouter(directory sp.Directory, cache sp.PlacementCache) *CachedRouter {
	return &CachedRouter{directory: directory, cache: cache}
}

func (r *CachedRouter) Lookup(ctx context.Context, key sp.GrainKey) (*sp.PlacementRoute, error) {
	r.mu.Lock()
	epoch := r.epoch
	if route, ok := r.cache.GetCachedPlacement(key); ok {
		if route.Status == sp.PlacementStatusActive && time.Now().Before(route.ValidUntil) {
			r.mu.Unlock()
			return route, nil
		}
		r.cache.DeleteCachedPlacement(key)
	}
	r.mu.Unlock()

	route, err := r.directory.Lookup(ctx, key)
	if err != nil {
		return nil, err
	}
	if route.Status != sp.PlacementStatusActive || !time.Now().Before(route.ValidUntil) {
		return nil, sp.ErrPlacementNotFound
	}
	r.mu.Lock()
	if r.epoch == epoch {
		r.cache.SetCachedPlacement(key, *route)
	}
	r.mu.Unlock()
	return route, nil
}

func (r *CachedRouter) Allocate(ctx context.Context, cmd sp.AllocateCommand) (*sp.Placement, error) {
	r.mu.Lock()
	epoch := r.epoch
	r.mu.Unlock()
	placement, err := r.directory.Allocate(ctx, cmd)
	if err != nil {
		return nil, err
	}
	route, lookupErr := r.directory.Lookup(ctx, placement.GrainKey)
	if lookupErr != nil || route.Status != sp.PlacementStatusActive || !time.Now().Before(route.ValidUntil) {
		return placement, nil
	}
	r.mu.Lock()
	if r.epoch == epoch || r.matchesSynchronousCreatedLocked(epoch, *placement) {
		r.cache.SetCachedPlacement(placement.GrainKey, *route)
	}
	r.mu.Unlock()
	return placement, nil
}

func (r *CachedRouter) HandleEvent(event sp.PlacementEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch event.Type {
	case sp.EventPlacementCreated,
		sp.EventPlacementReleased,
		sp.EventPlacementTransferred,
		sp.EventPlacementRecovered,
		sp.EventPlacementCacheInvalidated:
		r.advanceEventLocked(event)
		if event.GrainKey == "" {
			r.cache.ClearPlacementCache()
			return nil
		}
		r.cache.DeleteCachedPlacement(event.GrainKey)
	case sp.EventNodeReplaced,
		sp.EventNodeLeaseExpired,
		sp.EventNodeDraining,
		sp.EventNodeMarkedInvalid,
		sp.EventNodeUnregistered:
		r.advanceEventLocked(event)
		if event.NodeIdentity == "" {
			r.cache.ClearPlacementCache()
			return nil
		}
		r.cache.DeleteCachedPlacementsByNode(event.NodeIdentity)
	case sp.EventNodeRestored:
		r.advanceEventLocked(event)
		if event.NodeType == "" || event.NodeGroup == "" {
			r.cache.ClearPlacementCache()
			return nil
		}
		r.cache.DeleteCachedPlacementsByGroup(event.NodeType, event.NodeGroup)
	case sp.EventNodeRegistered:
		// A newly registered node cannot invalidate an existing placement route.
	case sp.EventPlacementRenewed:
		// Renew is an audit event and does not change route authorization.
	case sp.EventManualCacheClear:
		r.advanceEventLocked(event)
		r.cache.ClearPlacementCache()
	default:
		r.advanceEventLocked(event)
		r.cache.ClearPlacementCache()
	}
	return nil
}

func (r *CachedRouter) Degrade() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advanceLocked()
	r.cache.SetDegraded(true)
}

func (r *CachedRouter) Recover() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advanceLocked()
	r.cache.ClearPlacementCache()
	r.cache.SetDegraded(false)
}

func (r *CachedRouter) advanceEventLocked(event sp.PlacementEvent) {
	r.advanceLocked()
	r.lastEvent = event
	r.lastEventEpoch = r.epoch
}

func (r *CachedRouter) advanceLocked() {
	r.epoch++
	r.lastEvent = sp.PlacementEvent{}
	r.lastEventEpoch = 0
}

func (r *CachedRouter) matchesSynchronousCreatedLocked(epoch uint64, placement sp.Placement) bool {
	return r.epoch == epoch+1 &&
		r.lastEventEpoch == r.epoch &&
		r.lastEvent.Type == sp.EventPlacementCreated &&
		r.lastEvent.GrainKey == placement.GrainKey &&
		r.lastEvent.PlacementVersion > 0 &&
		r.lastEvent.PlacementVersion == placement.Version
}
