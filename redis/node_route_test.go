package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func TestRedisDirectoryAllocateLookupRenewAndReleaseRoute(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(500, 0))
	node := testNode("game-1", "session-a")
	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	placement, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "1", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if placement.OwnerNodeSessionID != node.NodeSessionID || placement.Version != 1 {
		t.Fatalf("placement = %+v", placement)
	}
	requestStart := time.Now()
	route, err := dir.Lookup(ctx, placement.GrainKey)
	if err != nil {
		t.Fatal(err)
	}
	if route.NodeLeaseVersion != 1 || route.ValidUntil.Before(requestStart.Add(900*time.Millisecond)) || route.ValidUntil.After(requestStart.Add(1100*time.Millisecond)) {
		t.Fatalf("route = %+v", route)
	}
	placementRaw := client.Get(ctx, PlacementKey(placement.GrainKey)).Val()
	nodeRaw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
	leaseScore := client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val()
	reliableEvents := client.XLen(ctx, EventsStreamKey()).Val()
	renewed, err := dir.Renew(ctx, sp.RenewCommand{GrainKey: placement.GrainKey, NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: placement.Version})
	if err != nil || renewed.Version != placement.Version {
		t.Fatalf("renew = %+v, %v", renewed, err)
	}
	if client.Get(ctx, PlacementKey(placement.GrainKey)).Val() != placementRaw || client.Get(ctx, NodeKey(node.NodeIdentity)).Val() != nodeRaw || client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val() != leaseScore {
		t.Fatal("renew changed business state")
	}
	if client.XLen(ctx, AuditStreamKey()).Val() != 1 {
		t.Fatal("renew did not write audit")
	}
	if client.XLen(ctx, EventsStreamKey()).Val() != reliableEvents {
		t.Fatal("renew wrote reliable cache invalidation event")
	}
	server.SetTime(time.Unix(501, 0))
	if _, err := dir.Lookup(ctx, placement.GrainKey); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("expired lookup = %v", err)
	}
	if _, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "1", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"}); !errors.Is(err, sp.ErrPlacementOwnerUnavailable) {
		t.Fatalf("allocate unavailable = %v", err)
	}
	if err := dir.Release(ctx, sp.ReleaseCommand{GrainKey: placement.GrainKey, NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: placement.Version}); err != nil {
		t.Fatal(err)
	}
}

type delayedEvalClient struct {
	goredis.UniversalClient
	delay time.Duration
}

func (c delayedEvalClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *goredis.Cmd {
	cmd := c.UniversalClient.Eval(ctx, script, keys, args...)
	if script == lookupLua {
		time.Sleep(c.delay)
	}
	return cmd
}

func TestRedisDirectoryLookupRejectsExpiredOnArrival(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: 20 * time.Millisecond})
	server.SetTime(time.Unix(600, 0))
	node := testNode("game-1", "session-a")
	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	p, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "slow", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	slow, err := NewDirectory(delayedEvalClient{UniversalClient: client, delay: 40 * time.Millisecond}, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := slow.Lookup(ctx, p.GrainKey); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestRedisDirectoryTransferAndRecoverUseCurrentTargetSession(t *testing.T) {
	ctx := context.Background()
	dir, _, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(700, 0))
	a, b := testNode("game-1", "session-a"), testNode("game-2", "session-b")
	for _, node := range []sp.Node{a, b} {
		if err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	p, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "2", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	target := b
	if p.NodeIdentity == b.NodeIdentity {
		target = a
	}
	moved, err := dir.Transfer(ctx, sp.TransferCommand{GrainKey: p.GrainKey, FromNodeIdentity: p.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: p.Version})
	if err != nil || moved.OwnerNodeSessionID != target.NodeSessionID {
		t.Fatalf("transfer = %+v, %v", moved, err)
	}
	if _, err := dir.Recover(ctx, sp.RecoverCommand{GrainKey: moved.GrainKey, NewNodeIdentity: p.NodeIdentity, PlacementVersion: moved.Version}); !errors.Is(err, sp.ErrPlacementNotRecoverable) {
		t.Fatalf("healthy recover = %v", err)
	}
	server.SetTime(time.UnixMilli(700500))
	if err := dir.RenewNode(ctx, p.NodeIdentity, p.OwnerNodeSessionID); err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.Unix(701, 0))
	recovered, err := dir.Recover(ctx, sp.RecoverCommand{GrainKey: moved.GrainKey, NewNodeIdentity: p.NodeIdentity, PlacementVersion: moved.Version})
	if err != nil || recovered.OwnerNodeSessionID == "" || recovered.Version != moved.Version+1 {
		t.Fatalf("recover = %+v, %v", recovered, err)
	}
}

func TestRedisDirectoryRenewAuditWrongTypeDoesNotChangeBusinessState(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(900, 0))
	node := testNode("game-1", "session-a")
	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	p, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "audit", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	praw := client.Get(ctx, PlacementKey(p.GrainKey)).Val()
	nraw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
	score := client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val()
	client.Set(ctx, AuditStreamKey(), "wrongtype", 0)
	_, err = dir.Renew(ctx, sp.RenewCommand{GrainKey: p.GrainKey, NodeIdentity: p.NodeIdentity, NodeSessionID: p.OwnerNodeSessionID, PlacementVersion: p.Version})
	if err == nil {
		t.Fatal("expected audit WRONGTYPE")
	}
	if client.Get(ctx, PlacementKey(p.GrainKey)).Val() != praw || client.Get(ctx, NodeKey(node.NodeIdentity)).Val() != nraw || client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val() != score {
		t.Fatal("renew audit failure changed business state")
	}
}

func TestRedisDirectoryTransferSequenceBoundaryIsAtomic(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(910, 0))
	a, b := testNode("game-1", "session-a"), testNode("game-2", "session-b")
	for _, node := range []sp.Node{a, b} {
		if err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	p, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "sequence", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	target := b
	if p.NodeIdentity == b.NodeIdentity {
		target = a
	}
	praw := client.Get(ctx, PlacementKey(p.GrainKey)).Val()
	oldScore := client.ZScore(ctx, PlacementNodeKey(p.NodeIdentity), p.GrainKey.String()).Val()
	events := client.XLen(ctx, EventsStreamKey()).Val()
	client.Set(ctx, SequenceKey(), "9007199254740991", 0)
	if _, err := dir.Transfer(ctx, sp.TransferCommand{GrainKey: p.GrainKey, FromNodeIdentity: p.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: p.Version}); err == nil {
		t.Fatal("expected sequence boundary error")
	}
	if client.Get(ctx, PlacementKey(p.GrainKey)).Val() != praw || client.ZScore(ctx, PlacementNodeKey(p.NodeIdentity), p.GrainKey.String()).Val() != oldScore || client.ZCard(ctx, PlacementNodeKey(target.NodeIdentity)).Val() != 0 || client.XLen(ctx, EventsStreamKey()).Val() != events {
		t.Fatal("sequence failure changed state")
	}
}
