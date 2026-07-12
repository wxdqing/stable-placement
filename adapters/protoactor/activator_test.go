package protoactor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/service/cluster"
	sp "github.com/wxdqing/stable-placement"
)

func TestSerialActivatorFenceStopsActivePIDsAndRejectsActivation(t *testing.T) {
	activator := NewSerialActivator(&resolverDirectory{}, "game/server-1/game-1", "session-1", "local", func(context.Context, *cluster.ClusterIdentity) (*actor.PID, error) {
		return actor.NewPID("local", "unused"), nil
	})
	first := actor.NewPID("local", "player-1")
	second := actor.NewPID("local", "player-2")
	activator.active["player/acct-1"] = PIDRoute{PID: first}
	activator.active["player/acct-2"] = PIDRoute{PID: second}
	var stopped []*actor.PID

	err := activator.Fence(context.Background(), func(_ context.Context, pid *actor.PID) error {
		stopped = append(stopped, pid)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(stopped) != 2 || len(activator.active) != 0 {
		t.Fatalf("stopped=%v active=%v", stopped, activator.active)
	}
	if _, err := activator.Activate(context.Background(), cluster.NewClusterIdentity("acct-3", "player"), sp.PlacementRoute{}); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("Activate after Fence error = %v", err)
	}
}

func TestSerialActivatorCreatesOnePIDForConcurrentRequests(t *testing.T) {
	lookup := &resolverDirectory{route: sp.PlacementRoute{GrainKey: "player/acct-1", NodeIdentity: "game/server-1/game-1", OwnerNodeSessionID: "s", Version: 1, Status: sp.PlacementStatusActive, ValidUntil: time.Now().Add(time.Minute)}}
	var mu sync.Mutex
	spawnCalls := 0
	activator := NewSerialActivator(lookup, "game/server-1/game-1", "s", "local", func(context.Context, *cluster.ClusterIdentity) (*actor.PID, error) {
		mu.Lock()
		defer mu.Unlock()
		spawnCalls++
		return actor.NewPID("local", "player-acct-1"), nil
	})
	route := lookup.route
	identity := cluster.NewClusterIdentity("acct-1", "player")
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := activator.Activate(context.Background(), identity, route); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if spawnCalls != 1 {
		t.Fatalf("spawn calls = %d", spawnCalls)
	}
}

func TestSerialActivatorRejectsExpectedRouteMismatch(t *testing.T) {
	lookup := &resolverDirectory{route: sp.PlacementRoute{GrainKey: "player/acct-1", NodeIdentity: "game/server-1/game-1", OwnerNodeSessionID: "s", Version: 2, Status: sp.PlacementStatusActive, ValidUntil: time.Now().Add(time.Minute)}}
	activator := NewSerialActivator(lookup, "game/server-1/game-1", "s", "local", func(context.Context, *cluster.ClusterIdentity) (*actor.PID, error) {
		return actor.NewPID("local", "p"), nil
	})
	stale := lookup.route
	stale.Version = 1
	if _, err := activator.Activate(context.Background(), cluster.NewClusterIdentity("acct-1", "player"), stale); err == nil {
		t.Fatal("stale activation succeeded")
	}
}

func TestSerialActivatorReadsLocalAddressAtActivationTime(t *testing.T) {
	lookup := &resolverDirectory{route: sp.PlacementRoute{GrainKey: "player/acct-1", NodeIdentity: "game/server-1/game-1", OwnerNodeSessionID: "s", Version: 1, ValidUntil: time.Now().Add(time.Minute)}}
	address := "before-remote-start"
	activator := NewSerialActivatorWithAddress(lookup, "game/server-1/game-1", "s", func() string { return address }, func(context.Context, *cluster.ClusterIdentity) (*actor.PID, error) {
		return actor.NewPID(address, "player-acct-1"), nil
	})
	address = "127.0.0.1:12001"
	pid, err := activator.Activate(context.Background(), cluster.NewClusterIdentity("acct-1", "player"), lookup.route)
	if err != nil || pid.Address != address {
		t.Fatalf("pid=%v err=%v", pid, err)
	}
}
