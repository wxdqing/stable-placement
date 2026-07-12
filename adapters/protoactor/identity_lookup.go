package protoactor

import (
	"context"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/service/cluster"
)

type IdentityLookup struct {
	resolver PIDResolver
	timeout  time.Duration
}

var _ cluster.IdentityLookup = (*IdentityLookup)(nil)

func NewIdentityLookup(resolver PIDResolver, timeout time.Duration) *IdentityLookup {
	if timeout <= 0 {
		timeout = time.Second
	}
	return &IdentityLookup{resolver: resolver, timeout: timeout}
}

func (l *IdentityLookup) Get(placementContext *cluster.PlacementContext, identity *cluster.ClusterIdentity) *actor.PID {
	if l == nil || l.resolver == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), l.timeout)
	defer cancel()
	route, err := l.resolver.ResolvePID(ctx, placementContext, identity)
	if err != nil {
		return nil
	}
	return route.PID
}

func (l *IdentityLookup) RemovePid(_ *cluster.PlacementContext, identity *cluster.ClusterIdentity, pid *actor.PID) {
	if l != nil && l.resolver != nil {
		l.resolver.Remove(identity, pid)
	}
}

func (*IdentityLookup) Setup(*cluster.Cluster, []string, bool) {}

func (l *IdentityLookup) Shutdown() {
	if resolver, ok := l.resolver.(interface{ Degrade() }); ok {
		resolver.Degrade()
	}
}
