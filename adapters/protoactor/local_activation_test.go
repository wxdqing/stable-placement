package protoactor

import (
	"context"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/service/cluster"
)

func TestLocalActivationResolverRejectsRemoteAndStaleRoutes(t *testing.T) {
	identity := cluster.NewClusterIdentity("acct-1", "player")
	expected := ExpectedRoute{PlacementID: "placement-1", NodeIdentity: "game/g/n", OwnerNodeSessionID: "s", PlacementVersion: 2}
	for _, test := range []struct {
		name  string
		route PIDRoute
	}{
		{name: "remote", route: PIDRoute{PID: actor.NewPID("remote", "p"), PlacementID: "placement-1", NodeIdentity: "game/g/n", NodeSessionID: "s", PlacementVersion: 2, ValidUntil: time.Now().Add(time.Minute)}},
		{name: "stale id", route: PIDRoute{PID: actor.NewPID("local", "p"), PlacementID: "placement-old", NodeIdentity: "game/g/n", NodeSessionID: "s", PlacementVersion: 2, ValidUntil: time.Now().Add(time.Minute)}},
		{name: "stale version", route: PIDRoute{PID: actor.NewPID("local", "p"), PlacementID: "placement-1", NodeIdentity: "game/g/n", NodeSessionID: "s", PlacementVersion: 1, ValidUntil: time.Now().Add(time.Minute)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := &fixedPIDResolver{route: test.route}
			local := NewLocalActivationResolver(resolver, "local")
			if _, err := local.ResolveLocalPID(context.Background(), nil, identity, expected); err == nil {
				t.Fatal("ResolveLocalPID succeeded")
			}
		})
	}
}
