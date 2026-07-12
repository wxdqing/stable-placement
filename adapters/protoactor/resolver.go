package protoactor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/service/cluster"
	sp "github.com/wxdqing/stable-placement"
)

type Resolver struct {
	directory RouteDirectory
	routes    map[string]sp.KindRouteConfig
	activator PIDActivator

	mu       sync.RWMutex
	cache    map[sp.GrainKey]PIDRoute
	degraded bool
	now      func() time.Time
}

func NewResolver(directory RouteDirectory, routes map[string]sp.KindRouteConfig, activator PIDActivator) *Resolver {
	cloned := make(map[string]sp.KindRouteConfig, len(routes))
	for kind, route := range routes {
		cloned[kind] = route
	}
	return &Resolver{directory: directory, routes: cloned, activator: activator, cache: make(map[sp.GrainKey]PIDRoute), now: time.Now}
}

func (r *Resolver) ResolvePID(ctx context.Context, placementContext *cluster.PlacementContext, identity *cluster.ClusterIdentity) (PIDRoute, error) {
	if r == nil || r.directory == nil || r.activator == nil || identity == nil {
		return PIDRoute{}, fmt.Errorf("stable placement protoactor resolver is not configured")
	}
	labels := map[string]string(nil)
	if placementContext != nil {
		labels = placementContext.Labels
	}
	target, err := sp.BuildNodeGroup(identity.Kind, labels, r.routes)
	if err != nil {
		return PIDRoute{}, err
	}
	if placementContext != nil && placementContext.NodeType != "" && placementContext.NodeType != target.NodeType {
		return PIDRoute{}, fmt.Errorf("%w: placement node type %q", sp.ErrPlacementConfigInvalid, placementContext.NodeType)
	}
	key, err := sp.NewGrainKey(identity.Kind, identity.Identity)
	if err != nil {
		return PIDRoute{}, err
	}
	now := r.now()
	r.mu.RLock()
	cached, found := r.cache[key]
	degraded := r.degraded
	r.mu.RUnlock()
	if found && !degraded && now.Before(cached.ValidUntil) && routeTargetMatches(cached.NodeIdentity, target) {
		return cached, nil
	}

	route, err := r.directory.ResolveRoute(ctx, sp.ResolveRouteCommand{
		GrainID: identity.Identity, Kind: identity.Kind,
		TargetNodeType: target.NodeType, TargetNodeGroup: target.NodeGroup,
	})
	if err != nil {
		return PIDRoute{}, err
	}
	if !r.now().Before(route.ValidUntil) {
		return PIDRoute{}, sp.ErrPlacementOwnerUnavailable
	}
	if found && sameAuthorization(cached, route) && cached.PID != nil {
		refreshed := pidRoute(cached.PID, route)
		if !degraded {
			r.store(refreshed)
		}
		return refreshed, nil
	}
	pid, err := r.activator.Activate(ctx, identity, *route)
	if err != nil {
		return PIDRoute{}, err
	}
	if pid == nil {
		return PIDRoute{}, fmt.Errorf("activation returned nil PID")
	}
	resolved := pidRoute(pid, route)
	if !degraded {
		r.store(resolved)
	}
	return resolved, nil
}

func (r *Resolver) Remove(identity *cluster.ClusterIdentity, pid *actor.PID) {
	if identity == nil {
		return
	}
	key, err := sp.NewGrainKey(identity.Kind, identity.Identity)
	if err != nil {
		return
	}
	r.mu.Lock()
	if current, ok := r.cache[key]; ok && (pid == nil || current.PID.Equal(pid)) {
		delete(r.cache, key)
	}
	r.mu.Unlock()
}

func (r *Resolver) HandleEvent(event sp.PlacementEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if event.GrainKey != "" {
		delete(r.cache, event.GrainKey)
		return
	}
	for key, route := range r.cache {
		if event.NodeIdentity == "" || route.NodeIdentity == event.NodeIdentity {
			delete(r.cache, key)
		}
	}
}

func (r *Resolver) Degrade() { r.mu.Lock(); r.degraded = true; clear(r.cache); r.mu.Unlock() }
func (r *Resolver) Recover() { r.mu.Lock(); clear(r.cache); r.degraded = false; r.mu.Unlock() }

func (r *Resolver) store(route PIDRoute) { r.mu.Lock(); r.cache[route.GrainKey] = route; r.mu.Unlock() }

func pidRoute(pid *actor.PID, route *sp.PlacementRoute) PIDRoute {
	return PIDRoute{PID: pid, GrainKey: route.GrainKey, NodeIdentity: route.NodeIdentity, NodeSessionID: route.OwnerNodeSessionID, PlacementVersion: route.Version, NodeLeaseVersion: route.NodeLeaseVersion, ValidUntil: route.ValidUntil}
}

func sameAuthorization(cached PIDRoute, route *sp.PlacementRoute) bool {
	return cached.GrainKey == route.GrainKey && cached.NodeIdentity == route.NodeIdentity && cached.NodeSessionID == route.OwnerNodeSessionID && cached.PlacementVersion == route.Version
}

func routeTargetMatches(identity string, target sp.NodeTarget) bool {
	id := sp.NodeIdentity(identity)
	return id.NodeType() == target.NodeType && id.NodeGroup() == target.NodeGroup
}
