package protoactor

import (
	"context"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/service/cluster"
	sp "github.com/wxdqing/stable-placement"
)

type LocalActivationResolver struct {
	resolver     PIDResolver
	localAddress string
}

var _ ActivationResolver = (*LocalActivationResolver)(nil)

func NewLocalActivationResolver(resolver PIDResolver, localAddress string) *LocalActivationResolver {
	return &LocalActivationResolver{resolver: resolver, localAddress: localAddress}
}

func (r *LocalActivationResolver) ResolveLocalPID(
	ctx context.Context,
	placementContext *cluster.PlacementContext,
	identity *cluster.ClusterIdentity,
	expected ExpectedRoute,
) (PIDRoute, error) {
	if r == nil || r.resolver == nil || r.localAddress == "" {
		return PIDRoute{}, fmt.Errorf("local activation resolver is not configured")
	}
	route, err := r.resolver.ResolvePID(ctx, placementContext, identity)
	if err != nil {
		return PIDRoute{}, err
	}
	if route.PlacementID != expected.PlacementID ||
		route.NodeIdentity != expected.NodeIdentity ||
		route.NodeSessionID != expected.OwnerNodeSessionID ||
		route.PlacementVersion != expected.PlacementVersion {
		return PIDRoute{}, sp.ErrVersionConflict
	}
	if route.PID == nil || route.PID.Address != r.localAddress {
		return PIDRoute{}, fmt.Errorf("resolved PID is not local")
	}
	if !time.Now().Before(route.ValidUntil) {
		return PIDRoute{}, sp.ErrPlacementOwnerUnavailable
	}
	return route, nil
}
