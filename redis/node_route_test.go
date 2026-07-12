package redis

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type routeMutationSnapshot struct {
	placement   string
	nodes       map[string]string
	leases      []goredis.Z
	nodeMembers []string
	indexes     map[string][]goredis.Z
	sequence    string
	events      []goredis.XMessage
	audit       []goredis.XMessage
}

func captureRouteMutationSnapshot(t *testing.T, ctx context.Context, client *goredis.Client, p sp.Placement, identities ...string) routeMutationSnapshot {
	t.Helper()
	s := routeMutationSnapshot{placement: client.Get(ctx, PlacementKey(p.GrainKey)).Val(), nodes: map[string]string{}, indexes: map[string][]goredis.Z{}, sequence: client.Get(ctx, SequenceKey()).Val()}
	var err error
	s.events, err = client.XRange(ctx, EventsStreamKey(), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	s.audit, err = client.XRange(ctx, AuditStreamKey(), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range identities {
		s.nodes[identity] = client.Get(ctx, NodeKey(identity)).Val()
		values, err := client.ZRangeWithScores(ctx, PlacementNodeKey(identity), 0, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		s.indexes[identity] = values
	}
	s.leases, _ = client.ZRangeWithScores(ctx, NodeLeaseKey("game", "default"), 0, -1).Result()
	s.nodeMembers, _ = client.SMembers(ctx, NodesKey("game", "default")).Result()
	sort.Strings(s.nodeMembers)
	return s
}

func requireRouteMutationSnapshot(t *testing.T, ctx context.Context, client *goredis.Client, p sp.Placement, want routeMutationSnapshot, identities ...string) {
	t.Helper()
	got := captureRouteMutationSnapshot(t, ctx, client, p, identities...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mutation snapshot changed\ngot:  %#v\nwant: %#v", got, want)
	}
}

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

func TestRedisDirectoryRecoverReleasedReturnsNotRecoverableV2(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(550, 0))
	a, b := testNode("game-1", "session-a"), testNode("game-2", "session-b")
	for _, node := range []sp.Node{a, b} {
		if err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	placement, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "released", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.Release(ctx, sp.ReleaseCommand{GrainKey: placement.GrainKey, NodeIdentity: placement.NodeIdentity, NodeSessionID: placement.OwnerNodeSessionID, PlacementVersion: placement.Version}); err != nil {
		t.Fatal(err)
	}
	target := a
	if placement.NodeIdentity == a.NodeIdentity {
		target = b
	}
	released := *placement
	released.Version++
	want := captureRouteMutationSnapshot(t, ctx, client, released, a.NodeIdentity, b.NodeIdentity)

	t.Run("current version", func(t *testing.T) {
		_, err := dir.Recover(ctx, sp.RecoverCommand{GrainKey: placement.GrainKey, NewNodeIdentity: target.NodeIdentity, PlacementVersion: released.Version})
		if !errors.Is(err, sp.ErrPlacementNotRecoverable) {
			t.Fatalf("Recover released placement err = %v, want ErrPlacementNotRecoverable", err)
		}
		requireRouteMutationSnapshot(t, ctx, client, released, want, a.NodeIdentity, b.NodeIdentity)
	})
	t.Run("stale version", func(t *testing.T) {
		_, err := dir.Recover(ctx, sp.RecoverCommand{GrainKey: placement.GrainKey, NewNodeIdentity: target.NodeIdentity, PlacementVersion: placement.Version})
		if !errors.Is(err, sp.ErrVersionConflict) {
			t.Fatalf("Recover released placement with stale version err = %v, want ErrVersionConflict", err)
		}
		requireRouteMutationSnapshot(t, ctx, client, released, want, a.NodeIdentity, b.NodeIdentity)
	})
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

func TestRedisDirectoryValidUntilConservativelyIncludesResponseDelay(t *testing.T) {
	ctx := context.Background()
	const ttl = 5 * time.Second
	const delay = 100 * time.Millisecond
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: ttl})
	server.SetTime(time.Unix(610, 0))
	node := testNode("game-1", "session-a")
	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	p, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "delayed", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	slow, err := NewDirectory(delayedEvalClient{UniversalClient: client, delay: delay}, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	route, err := slow.Lookup(ctx, p.GrainKey)
	if err != nil {
		t.Fatal(err)
	}
	remaining := time.Until(route.ValidUntil)
	if remaining <= 0 || remaining >= ttl-delay/2 {
		t.Fatalf("remaining ValidUntil window = %v, want positive and shortened by response delay", remaining)
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

func TestRedisDirectoryReleaseRejectsOwnerNodeKeyMismatchWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(1050, 0))
	node := testNode("game-1", "session-a")
	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	p, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "release-metadata", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	var stored redisNode
	if err := json.Unmarshal([]byte(client.Get(ctx, NodeKey(node.NodeIdentity)).Val()), &stored); err != nil {
		t.Fatal(err)
	}
	stored.NodeKey = NodeKey("other/default/node")
	changed, _ := json.Marshal(stored)
	client.Set(ctx, NodeKey(node.NodeIdentity), changed, 0)
	before := captureRouteMutationSnapshot(t, ctx, client, *p, node.NodeIdentity)
	if err := dir.Release(ctx, sp.ReleaseCommand{GrainKey: p.GrainKey, NodeIdentity: p.NodeIdentity, NodeSessionID: p.OwnerNodeSessionID, PlacementVersion: p.Version}); err == nil {
		t.Fatal("expected owner NodeKey mismatch")
	}
	requireRouteMutationSnapshot(t, ctx, client, *p, before, node.NodeIdentity)
}

func TestRedisDirectoryTransferRecoverRejectTargetMetadataChangedBeforeEval(t *testing.T) {
	for _, mode := range []string{"transfer", "recover"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			base, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
			server.SetTime(time.Unix(1060, 0))
			a, b := testNode("game-1", "session-a"), testNode("game-2", "session-b")
			for _, node := range []sp.Node{a, b} {
				if err := base.RegisterNode(ctx, node); err != nil {
					t.Fatal(err)
				}
			}
			p, err := base.Allocate(ctx, sp.AllocateCommand{GrainID: "metadata-" + mode, Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			if err != nil {
				t.Fatal(err)
			}
			target := b
			if p.NodeIdentity == b.NodeIdentity {
				target = a
			}
			if mode == "recover" {
				var owner redisNode
				if err := json.Unmarshal([]byte(client.Get(ctx, NodeKey(p.NodeIdentity)).Val()), &owner); err != nil {
					t.Fatal(err)
				}
				owner.Status = sp.NodeStatusOffline
				raw, _ := json.Marshal(owner)
				client.Set(ctx, NodeKey(p.NodeIdentity), raw, 0)
			}
			armed := true
			hooked, err := NewDirectory(nodeLeaseEvalHookClient{UniversalClient: client, before: func(script string) {
				if armed && script == mutationLua {
					armed = false
					var changed redisNode
					if err := json.Unmarshal([]byte(client.Get(ctx, NodeKey(target.NodeIdentity)).Val()), &changed); err != nil {
						t.Fatal(err)
					}
					changed.NodeGroup = "changed-after-read"
					raw, _ := json.Marshal(changed)
					client.Set(ctx, NodeKey(target.NodeIdentity), raw, 0)
				}
			}}, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			before := captureRouteMutationSnapshot(t, ctx, client, *p, p.NodeIdentity, target.NodeIdentity)
			if mode == "transfer" {
				_, err = hooked.Transfer(ctx, sp.TransferCommand{GrainKey: p.GrainKey, FromNodeIdentity: p.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: p.Version})
			} else {
				_, err = hooked.Recover(ctx, sp.RecoverCommand{GrainKey: p.GrainKey, NewNodeIdentity: target.NodeIdentity, PlacementVersion: p.Version})
			}
			if err == nil {
				t.Fatal("expected target metadata error")
			}
			// The hook's metadata write is expected; business state and outbox must otherwise remain unchanged.
			before.nodes[target.NodeIdentity] = client.Get(ctx, NodeKey(target.NodeIdentity)).Val()
			requireRouteMutationSnapshot(t, ctx, client, *p, before, p.NodeIdentity, target.NodeIdentity)
		})
	}
}
