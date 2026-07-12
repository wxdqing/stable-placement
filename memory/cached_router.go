package memory

import (
	"context"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

type CachedRouter struct {
	directory sp.Directory
	cache     sp.PlacementCache
}

func NewCachedRouter(directory sp.Directory, cache sp.PlacementCache) *CachedRouter {
	return &CachedRouter{directory: directory, cache: cache}
}

func (r *CachedRouter) Lookup(ctx context.Context, key sp.GrainKey) (*sp.PlacementRoute, error) {
	if route, ok := r.cache.GetCachedPlacement(key); ok {
		if route.Status == sp.PlacementStatusActive && time.Now().Before(route.LeaseExpireAt) {
			return route, nil
		}
		r.cache.DeleteCachedPlacement(key)
	}

	placement, err := r.directory.Lookup(ctx, key)
	if err != nil {
		return nil, err
	}
	route := placementRoute(*placement)
	r.cache.SetCachedPlacement(key, route)
	return &route, nil
}

func (r *CachedRouter) Allocate(ctx context.Context, cmd sp.AllocateCommand) (*sp.Placement, error) {
	placement, err := r.directory.Allocate(ctx, cmd)
	if err != nil {
		return nil, err
	}
	r.cache.SetCachedPlacement(placement.GrainKey, placementRoute(*placement))
	return placement, nil
}

func (r *CachedRouter) HandleEvent(event sp.PlacementEvent) error {
	switch event.Type {
	case sp.EventPlacementCreated,
		sp.EventPlacementRenewed,
		sp.EventPlacementReleased,
		sp.EventPlacementTransferred,
		sp.EventPlacementRecovered,
		sp.EventLeaseExpired,
		sp.EventPlacementCacheInvalidated:
		if event.GrainKey == "" {
			r.cache.ClearPlacementCache()
			return nil
		}
		r.cache.DeleteCachedPlacement(event.GrainKey)
	case sp.EventNodeReplaced,
		sp.EventNodeDraining,
		sp.EventNodeMarkedInvalid,
		sp.EventNodeUnregistered:
		if event.NodeIdentity == "" {
			r.cache.ClearPlacementCache()
			return nil
		}
		r.cache.DeleteCachedPlacementsByNode(event.NodeIdentity)
	case sp.EventNodeRestored:
		if event.NodeType == "" || event.NodeGroup == "" {
			r.cache.ClearPlacementCache()
			return nil
		}
		r.cache.DeleteCachedPlacementsByGroup(event.NodeType, event.NodeGroup)
	case sp.EventNodeRegistered:
		// A newly registered node cannot invalidate an existing placement route.
	case sp.EventManualCacheClear:
		r.cache.ClearPlacementCache()
	default:
		r.cache.ClearPlacementCache()
	}
	return nil
}

func (r *CachedRouter) Degrade() {
	r.cache.SetDegraded(true)
}

func (r *CachedRouter) Recover() {
	r.cache.ClearPlacementCache()
	r.cache.SetDegraded(false)
}

func placementRoute(placement sp.Placement) sp.PlacementRoute {
	return sp.PlacementRoute{
		GrainKey:      placement.GrainKey,
		NodeIdentity:  placement.NodeIdentity,
		Version:       placement.Version,
		Status:        placement.Status,
		LeaseExpireAt: placement.LeaseExpireAt,
	}
}
