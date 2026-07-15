package protoactor

import (
	"context"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/service/cluster"
	sp "github.com/wxdqing/stable-placement"
)

type PIDRoute struct {
	PID              *actor.PID
	GrainKey         sp.GrainKey
	PlacementID      string
	NodeIdentity     string
	NodeSessionID    string
	PlacementVersion int64
	NodeLeaseVersion int64
	ValidUntil       time.Time
}

type PIDResolver interface {
	ResolvePID(context.Context, *cluster.PlacementContext, *cluster.ClusterIdentity) (PIDRoute, error)
	Remove(*cluster.ClusterIdentity, *actor.PID)
}

type PIDActivator interface {
	Activate(context.Context, *cluster.ClusterIdentity, sp.PlacementRoute) (*actor.PID, error)
}

type ExpectedRoute struct {
	PlacementID        string
	NodeIdentity       string
	OwnerNodeSessionID string
	PlacementVersion   int64
}

type ActivationResolver interface {
	ResolveLocalPID(
		context.Context,
		*cluster.PlacementContext,
		*cluster.ClusterIdentity,
		ExpectedRoute,
	) (PIDRoute, error)
}

type RouteDirectory interface {
	ResolveRoute(context.Context, sp.ResolveRouteCommand) (*sp.PlacementRoute, error)
	Lookup(context.Context, sp.GrainKey) (*sp.PlacementRoute, error)
}
