package protoactor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/service/cluster"
	sp "github.com/wxdqing/stable-placement"
)

type SpawnFunc func(context.Context, *cluster.ClusterIdentity) (*actor.PID, error)

type SerialActivator struct {
	directory                   RouteDirectory
	nodeIdentity, nodeSessionID string
	localAddress                func() string
	spawn                       SpawnFunc
	mu                          sync.Mutex
	active                      map[sp.GrainKey]PIDRoute
	fenced                      bool
}

func NewSerialActivator(directory RouteDirectory, nodeIdentity, nodeSessionID, localAddress string, spawn SpawnFunc) *SerialActivator {
	return NewSerialActivatorWithAddress(directory, nodeIdentity, nodeSessionID, func() string { return localAddress }, spawn)
}

func NewSerialActivatorWithAddress(directory RouteDirectory, nodeIdentity, nodeSessionID string, localAddress func() string, spawn SpawnFunc) *SerialActivator {
	return &SerialActivator{directory: directory, nodeIdentity: nodeIdentity, nodeSessionID: nodeSessionID, localAddress: localAddress, spawn: spawn, active: make(map[sp.GrainKey]PIDRoute)}
}

func (a *SerialActivator) Activate(ctx context.Context, identity *cluster.ClusterIdentity, expected sp.PlacementRoute) (*actor.PID, error) {
	if a == nil || a.directory == nil || a.spawn == nil || a.localAddress == nil || identity == nil {
		return nil, fmt.Errorf("serial activator is not configured")
	}
	if expected.NodeIdentity != a.nodeIdentity || expected.OwnerNodeSessionID != a.nodeSessionID {
		return nil, sp.ErrInvalidNodeSession
	}
	key, err := sp.NewGrainKey(identity.Kind, identity.Identity)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fenced {
		return nil, sp.ErrInvalidNodeSession
	}
	current, err := a.directory.Lookup(ctx, key)
	if err != nil {
		return nil, err
	}
	if current.PlacementID != expected.PlacementID || current.NodeIdentity != expected.NodeIdentity || current.OwnerNodeSessionID != expected.OwnerNodeSessionID || current.Version != expected.Version || !time.Now().Before(current.ValidUntil) {
		return nil, sp.ErrVersionConflict
	}
	if route, ok := a.active[key]; ok && sameAuthorization(route, current) && route.PID != nil {
		return route.PID, nil
	}
	pid, err := a.spawn(ctx, identity)
	if err != nil {
		return nil, err
	}
	if pid == nil || pid.Address != a.localAddress() {
		return nil, fmt.Errorf("activation returned non-local PID")
	}
	a.active[key] = pidRoute(pid, current)
	return pid, nil
}

// Fence irreversibly rejects new activations and waits for every PID created
// by this node session to stop.
func (a *SerialActivator) Fence(ctx context.Context, stop func(context.Context, *actor.PID) error) error {
	if a == nil || stop == nil {
		return fmt.Errorf("serial activator fence is not configured")
	}
	a.mu.Lock()
	a.fenced = true
	pids := make([]*actor.PID, 0, len(a.active))
	for _, route := range a.active {
		if route.PID != nil {
			pids = append(pids, route.PID)
		}
	}
	clear(a.active)
	a.mu.Unlock()

	var result error
	for _, pid := range pids {
		if err := stop(ctx, pid); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (a *SerialActivator) Remove(identity *cluster.ClusterIdentity, pid *actor.PID) {
	if identity == nil {
		return
	}
	key, err := sp.NewGrainKey(identity.Kind, identity.Identity)
	if err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if current, ok := a.active[key]; ok && (pid == nil || current.PID.Equal(pid)) {
		delete(a.active, key)
	}
}
