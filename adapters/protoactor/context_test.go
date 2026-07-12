package protoactor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/remote"
	"github.com/asynkron/protoactor-go/service/cluster"
)

type fixedPIDResolver struct {
	mu    sync.Mutex
	calls int
	route PIDRoute
}

func (r *fixedPIDResolver) ResolvePID(context.Context, *cluster.PlacementContext, *cluster.ClusterIdentity) (PIDRoute, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return r.route, nil
}
func (r *fixedPIDResolver) Remove(*cluster.ClusterIdentity, *actor.PID) {}

type noOpProvider struct{}

func (noOpProvider) StartMember(*cluster.Cluster) error { return nil }
func (noOpProvider) StartClient(*cluster.Cluster) error { return nil }
func (noOpProvider) Shutdown(bool) error                { return nil }

type noOpLookup struct{}

func (noOpLookup) Get(*cluster.PlacementContext, *cluster.ClusterIdentity) *actor.PID        { return nil }
func (noOpLookup) RemovePid(*cluster.PlacementContext, *cluster.ClusterIdentity, *actor.PID) {}
func (noOpLookup) Setup(*cluster.Cluster, []string, bool)                                    {}
func (noOpLookup) Shutdown()                                                                 {}

func TestContextBypassesProtoactorPermanentPIDCache(t *testing.T) {
	system := actor.NewActorSystem()
	good := system.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {
		if message, ok := ctx.Message().(string); ok {
			ctx.Respond("reply:" + message)
		}
	}))
	resolver := &fixedPIDResolver{route: PIDRoute{PID: good, GrainKey: "player/acct-1", NodeIdentity: "game/g/n", NodeSessionID: "s", PlacementVersion: 1, NodeLeaseVersion: 1, ValidUntil: time.Now().Add(time.Minute)}}
	config := cluster.Configure("test", noOpProvider{}, noOpLookup{}, remote.Configure("127.0.0.1", 0), cluster.WithClusterContextProducer(NewContext(resolver)))
	c := cluster.New(system, config)
	c.PidCache.Set("acct-1", "player", actor.NewPID("stale", "pid"))

	response, err := c.Request(nil, "acct-1", "player", "ping", cluster.WithTimeout(time.Second), cluster.WithRetryCount(1))
	if err != nil || response != "reply:ping" {
		t.Fatalf("response=%v err=%v", response, err)
	}
	resolver.mu.Lock()
	calls := resolver.calls
	resolver.mu.Unlock()
	if calls != 1 {
		t.Fatalf("resolver calls = %d", calls)
	}
}

func TestIdentityLookupUsesSharedPIDResolver(t *testing.T) {
	pid := actor.NewPID("local", "player")
	resolver := &fixedPIDResolver{route: PIDRoute{PID: pid, ValidUntil: time.Now().Add(time.Minute)}}
	lookup := NewIdentityLookup(resolver, time.Second)
	got := lookup.Get(&cluster.PlacementContext{}, cluster.NewClusterIdentity("acct-1", "player"))
	if got != pid {
		t.Fatalf("pid=%v want=%v", got, pid)
	}
	resolver.mu.Lock()
	calls := resolver.calls
	resolver.mu.Unlock()
	if calls != 1 {
		t.Fatalf("resolver calls = %d", calls)
	}
}
