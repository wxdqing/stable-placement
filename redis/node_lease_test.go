package redis

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type leaseBatchSnapshot struct {
	firstRaw     string
	leaseMembers []goredis.Z
	nodeMembers  []string
	events       []goredis.XMessage
	audit        []goredis.XMessage
}

func captureLeaseBatchSnapshot(t *testing.T, ctx context.Context, client *goredis.Client, firstIdentity string) leaseBatchSnapshot {
	t.Helper()
	members, err := client.ZRangeWithScores(ctx, NodeLeaseKey("game", "default"), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := client.SMembers(ctx, NodesKey("game", "default")).Result()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(nodes)
	events, err := client.XRange(ctx, EventsStreamKey(), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	audit, err := client.XRange(ctx, AuditStreamKey(), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	return leaseBatchSnapshot{firstRaw: client.Get(ctx, NodeKey(firstIdentity)).Val(), leaseMembers: members, nodeMembers: nodes, events: events, audit: audit}
}

func requireLeaseBatchSnapshot(t *testing.T, ctx context.Context, client *goredis.Client, firstIdentity string, want leaseBatchSnapshot) {
	t.Helper()
	got := captureLeaseBatchSnapshot(t, ctx, client, firstIdentity)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("batch snapshot changed\ngot:  %#v\nwant: %#v", got, want)
	}
}

type nodeLeaseEvalHookClient struct {
	goredis.UniversalClient
	before func(string)
}

func (c nodeLeaseEvalHookClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *goredis.Cmd {
	if c.before != nil {
		c.before(script)
	}
	return c.UniversalClient.Eval(ctx, script, keys, args...)
}

func TestRedisDirectoryRegisterAndRenewNodeLease(t *testing.T) {
	ctx := context.Background()
	dir, _, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: 1500 * time.Microsecond})
	server.SetTime(time.Unix(100, 0))
	node := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	registered := readNode(t, ctx, dir, node.NodeIdentity)
	if registered.Status != sp.NodeStatusActive || registered.Lease.Version != 1 || registered.Lease.TTLMillis != 2 || registered.Lease.ExpireAtUnixMilli != 100002 {
		t.Fatalf("registered = %+v", registered)
	}
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatalf("idempotent register: %v", err)
	}
	if got := readNode(t, ctx, dir, node.NodeIdentity); got.Lease != registered.Lease {
		t.Fatalf("register renewed lease: %+v", got.Lease)
	}
	server.SetTime(time.UnixMilli(100001))
	if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
		t.Fatal(err)
	}
	renewed := readNode(t, ctx, dir, node.NodeIdentity)
	if renewed.Lease.Version != 2 || renewed.Lease.TTLMillis != 2 || renewed.Lease.ExpireAtUnixMilli != 100003 {
		t.Fatalf("renewed = %+v", renewed)
	}
}

func TestRedisDirectoryRenewMetricsAtomically(t *testing.T) {
	ctx := context.Background()
	dir, _, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: 10 * time.Second})
	server.SetTime(time.Unix(150, 0))
	node := testNode("game-1", "session-a")
	node.Metrics = sp.NodeMetrics{CPUAvailableMilliCores: 999, UpdatedAtUnixMilli: 1}
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	if got := readNode(t, ctx, dir, node.NodeIdentity); got.Metrics != (sp.NodeMetrics{}) {
		t.Fatalf("register accepted untrusted metrics: %+v", got.Metrics)
	}

	metrics := sp.NodeMetrics{CPUAvailableMilliCores: 500, MemoryAvailableBytes: 1 << 30, Goroutines: 20, UpdatedAtUnixMilli: 1}
	server.SetTime(time.UnixMilli(151234))
	if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, Metrics: &metrics}); err != nil {
		t.Fatal(err)
	}
	withMetrics := readNode(t, ctx, dir, node.NodeIdentity)
	if withMetrics.Metrics.CPUAvailableMilliCores != 500 || withMetrics.Metrics.MemoryAvailableBytes != 1<<30 || withMetrics.Metrics.Goroutines != 20 || withMetrics.Metrics.UpdatedAtUnixMilli != 151234 {
		t.Fatalf("renewed metrics = %+v", withMetrics.Metrics)
	}

	server.SetTime(time.UnixMilli(152000))
	if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
		t.Fatal(err)
	}
	withoutMetrics := readNode(t, ctx, dir, node.NodeIdentity)
	if withoutMetrics.Metrics != withMetrics.Metrics || withoutMetrics.Lease.Version != withMetrics.Lease.Version+1 {
		t.Fatalf("nil-metrics renew = %+v", withoutMetrics)
	}

	before := withoutMetrics
	bad := sp.NodeMetrics{MemoryAvailableBytes: -1}
	for _, cmd := range []sp.RenewNodeCommand{
		{NodeIdentity: node.NodeIdentity, NodeSessionID: "old-session", Metrics: &bad},
		{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, Metrics: &bad},
	} {
		if _, err := dir.RenewNode(ctx, cmd); err == nil {
			t.Fatalf("RenewNode(%+v) succeeded", cmd)
		}
		if after := readNode(t, ctx, dir, node.NodeIdentity); after != before {
			t.Fatalf("failed renew changed node: before=%+v after=%+v", before, after)
		}
	}
}

func TestRedisDirectoryRenewUsesPersistedTTL(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: 5 * time.Second})
	server.SetTime(time.Unix(200, 0))
	node := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	other, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.UnixMilli(201000))
	if _, err := other.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
		t.Fatal(err)
	}
	got := readNode(t, ctx, other, node.NodeIdentity)
	if got.Lease.TTLMillis != 5000 || got.Lease.ExpireAtUnixMilli != 206000 {
		t.Fatalf("lease = %+v", got.Lease)
	}
}

func TestRedisDirectoryRenewRejectsExpiredOfflineAndOldSession(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(300, 0))
	node := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: "old"}); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("old session: %v", err)
	}
	server.SetTime(time.Unix(301, 0))
	if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); !errors.Is(err, sp.ErrNodeLeaseExpired) {
		t.Fatalf("expired: %v", err)
	}
	raw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
	var stored redisNode
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatal(err)
	}
	stored.Status = sp.NodeStatusOffline
	b, _ := json.Marshal(stored)
	client.Set(ctx, NodeKey(node.NodeIdentity), b, 0)
	if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); !errors.Is(err, sp.ErrNodeNotFound) {
		t.Fatalf("offline: %v", err)
	}
}

func TestExpireNodeLeasesUsesRedisTimeAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(400, 0))
	for _, node := range []sp.Node{testNode("game-1", "session-a"), testNode("game-2", "session-b")} {
		if _, err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	server.SetTime(time.Unix(401, 0))
	count, err := dir.ExpireNodeLeases(ctx, "game", "default", 1)
	if err != nil || count != 1 {
		t.Fatalf("first scan = %d, %v", count, err)
	}
	count, err = dir.ExpireNodeLeases(ctx, "game", "default", 10)
	if err != nil || count != 1 {
		t.Fatalf("second scan = %d, %v", count, err)
	}
	count, err = dir.ExpireNodeLeases(ctx, "game", "default", 10)
	if err != nil || count != 0 {
		t.Fatalf("repeat scan = %d, %v", count, err)
	}
	events := client.XRange(ctx, EventsStreamKey(), "-", "+").Val()
	expired := 0
	for _, event := range events {
		if event.Values["type"] == string(sp.EventNodeLeaseExpired) {
			expired++
		}
	}
	if expired != 2 {
		t.Fatalf("expired events = %d", expired)
	}
}

func TestExpiredOfflineNodeTombstoneLifecycle(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T, retainPlacement bool) (*Directory, *goredis.Client, sp.Node, *sp.Placement) {
		t.Helper()
		dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
		server.SetTime(time.Unix(450, 0))
		node := testNode("game-1", "session-a")
		if _, err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
		var placement *sp.Placement
		if retainPlacement {
			var err error
			placement, err = dir.Allocate(ctx, sp.AllocateCommand{GrainID: "retained", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			if err != nil {
				t.Fatal(err)
			}
		}
		server.SetTime(time.Unix(451, 0))
		if count, err := dir.ExpireNodeLeases(ctx, "game", "default", 1); err != nil || count != 1 {
			t.Fatalf("ExpireNodeLeases = %d, %v", count, err)
		}
		stored := readNode(t, ctx, dir, node.NodeIdentity)
		if stored.Status != sp.NodeStatusOffline || client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Err() != goredis.Nil {
			t.Fatalf("expired node = %+v, lease score err = %v", stored, client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Err())
		}
		return dir, client, node, placement
	}
	keys := func(node sp.Node) []string {
		return []string{NodeKey(node.NodeIdentity), NodesKey(node.NodeType, node.NodeGroup), NodeLeaseKey(node.NodeType, node.NodeGroup), PlacementNodeKey(node.NodeIdentity), EventsStreamKey(), AuditStreamKey()}
	}

	t.Run("same-session register reports expired", func(t *testing.T) {
		dir, client, node, _ := setup(t, false)
		before := snapshotRedisKeysV2(t, client, keys(node)...)
		if _, err := dir.RegisterNode(ctx, node); !errors.Is(err, sp.ErrNodeLeaseExpired) {
			t.Fatalf("RegisterNode err = %v", err)
		}
		requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, client, keys(node)...), before)
	})

	t.Run("different-session register preserves session precedence", func(t *testing.T) {
		dir, client, node, _ := setup(t, false)
		before := snapshotRedisKeysV2(t, client, keys(node)...)
		node.NodeSessionID = "session-b"
		if _, err := dir.RegisterNode(ctx, node); !errors.Is(err, sp.ErrInvalidNodeSession) {
			t.Fatalf("RegisterNode err = %v", err)
		}
		requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, client, keys(node)...), before)
	})

	t.Run("renew reports offline backend error", func(t *testing.T) {
		dir, client, node, _ := setup(t, false)
		before := snapshotRedisKeysV2(t, client, keys(node)...)
		if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); !errors.Is(err, sp.ErrNodeNotFound) {
			t.Fatalf("RenewNode err = %v", err)
		}
		requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, client, keys(node)...), before)
	})

	t.Run("drain rejects offline tombstone", func(t *testing.T) {
		dir, client, node, _ := setup(t, false)
		if err := dir.MarkNodeInvalid(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
			t.Fatal(err)
		}
		before := snapshotRedisKeysV2(t, client, append(keys(node), InvalidNodesKey(node.NodeType, node.NodeGroup))...)
		if err := dir.DrainNode(ctx, node.NodeIdentity); !errors.Is(err, sp.ErrNodeNotFound) {
			t.Fatalf("DrainNode err = %v", err)
		}
		requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, client, append(keys(node), InvalidNodesKey(node.NodeType, node.NodeGroup))...), before)
	})

	t.Run("replace creates a new active lease", func(t *testing.T) {
		dir, client, node, _ := setup(t, false)
		beforeEvents := client.XLen(ctx, EventsStreamKey()).Val()
		replacement := node
		replacement.NodeSessionID = "session-b"
		old, _, err := dir.ReplaceNodeSession(ctx, replacement)
		if err != nil {
			t.Fatal(err)
		}
		stored := readNode(t, ctx, dir, node.NodeIdentity)
		score, scoreErr := client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Result()
		if old.NodeSessionID != node.NodeSessionID || stored.NodeSessionID != replacement.NodeSessionID || stored.Status != sp.NodeStatusActive || stored.Lease.Version != 1 || scoreErr != nil || int64(score) != stored.Lease.ExpireAtUnixMilli {
			t.Fatalf("old=%+v stored=%+v score=%v scoreErr=%v", old, stored, score, scoreErr)
		}
		if !client.SIsMember(ctx, NodesKey("game", "default"), NodeKey(node.NodeIdentity)).Val() || client.ZCard(ctx, PlacementNodeKey(node.NodeIdentity)).Val() != 0 || client.XLen(ctx, EventsStreamKey()).Val() != beforeEvents+1 {
			t.Fatal("replace did not atomically preserve indexes and append one event")
		}
		event := client.XRevRangeN(ctx, EventsStreamKey(), "+", "-", 1).Val()[0]
		if event.Values["type"] != string(sp.EventNodeReplaced) || event.Values["node_session_id"] != replacement.NodeSessionID {
			t.Fatalf("replace event = %+v", event.Values)
		}
	})

	for _, operation := range []string{"unregister", "complete drain"} {
		t.Run(operation+" deletes empty tombstone", func(t *testing.T) {
			dir, client, node, _ := setup(t, false)
			beforeEvents := client.XLen(ctx, EventsStreamKey()).Val()
			var err error
			if operation == "unregister" {
				err = dir.UnregisterNode(ctx, node.NodeIdentity, node.NodeSessionID)
			} else {
				err = dir.CompleteDrain(ctx, node.NodeIdentity, node.NodeSessionID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if client.Exists(ctx, NodeKey(node.NodeIdentity)).Val() != 0 || client.SIsMember(ctx, NodesKey("game", "default"), NodeKey(node.NodeIdentity)).Val() || client.ZCard(ctx, NodeLeaseKey("game", "default")).Val() != 0 || client.ZCard(ctx, PlacementNodeKey(node.NodeIdentity)).Val() != 0 || client.XLen(ctx, EventsStreamKey()).Val() != beforeEvents+1 {
				t.Fatal("delete did not atomically remove node indexes and append one event")
			}
		})
	}

	t.Run("complete drain retains tombstone with placements", func(t *testing.T) {
		dir, client, node, placement := setup(t, true)
		before := snapshotRedisKeysV2(t, client, append(keys(node), PlacementKey(placement.GrainKey))...)
		if err := dir.CompleteDrain(ctx, node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeHasPlacements) {
			t.Fatalf("CompleteDrain err = %v", err)
		}
		requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, client, append(keys(node), PlacementKey(placement.GrainKey))...), before)
	})
}

func TestRedisNodeLifecycleLeaseScoreRulesAreAtomic(t *testing.T) {
	ctx := context.Background()
	operations := []struct {
		name string
		run  func(context.Context, *Directory, sp.Node) error
	}{
		{name: "register", run: func(ctx context.Context, dir *Directory, node sp.Node) error {
			_, err := dir.RegisterNode(ctx, node)
			return err
		}},
		{name: "renew", run: func(ctx context.Context, dir *Directory, node sp.Node) error {
			_, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID})
			return err
		}},
		{name: "replace", run: func(ctx context.Context, dir *Directory, node sp.Node) error {
			node.NodeSessionID = "replacement"
			_, _, err := dir.ReplaceNodeSession(ctx, node)
			return err
		}},
		{name: "unregister", run: func(ctx context.Context, dir *Directory, node sp.Node) error {
			return dir.UnregisterNode(ctx, node.NodeIdentity, node.NodeSessionID)
		}},
		{name: "complete drain", run: func(ctx context.Context, dir *Directory, node sp.Node) error {
			return dir.CompleteDrain(ctx, node.NodeIdentity, node.NodeSessionID)
		}},
		{name: "drain", run: func(ctx context.Context, dir *Directory, node sp.Node) error {
			return dir.DrainNode(ctx, node.NodeIdentity)
		}},
	}
	for _, status := range []sp.NodeStatus{sp.NodeStatusActive, sp.NodeStatusDraining, sp.NodeStatusOffline} {
		for _, operation := range operations {
			t.Run(string(status)+"/"+operation.name, func(t *testing.T) {
				dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
				server.SetTime(time.Unix(460, 0))
				node := testNode("score-rules", "session-a")
				if _, err := dir.RegisterNode(ctx, node); err != nil {
					t.Fatal(err)
				}
				if status == sp.NodeStatusOffline {
					server.SetTime(time.Unix(461, 0))
					if count, err := dir.ExpireNodeLeases(ctx, node.NodeType, node.NodeGroup, 1); err != nil || count != 1 {
						t.Fatalf("ExpireNodeLeases = %d, %v", count, err)
					}
					client.ZAdd(ctx, NodeLeaseKey(node.NodeType, node.NodeGroup), goredis.Z{Score: 1, Member: NodeKey(node.NodeIdentity)})
				} else {
					if status == sp.NodeStatusDraining {
						var stored redisNode
						if err := json.Unmarshal([]byte(client.Get(ctx, NodeKey(node.NodeIdentity)).Val()), &stored); err != nil {
							t.Fatal(err)
						}
						stored.Status = status
						raw, err := json.Marshal(stored)
						if err != nil {
							t.Fatal(err)
						}
						client.Set(ctx, NodeKey(node.NodeIdentity), raw, 0)
					}
					client.ZRem(ctx, NodeLeaseKey(node.NodeType, node.NodeGroup), NodeKey(node.NodeIdentity))
				}
				if operation.name == "drain" {
					if err := dir.MarkNodeInvalid(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
						t.Fatal(err)
					}
				}
				keys := []string{NodeKey(node.NodeIdentity), NodesKey(node.NodeType, node.NodeGroup), NodeLeaseKey(node.NodeType, node.NodeGroup), PlacementNodeKey(node.NodeIdentity), InvalidNodesKey(node.NodeType, node.NodeGroup), EventsStreamKey(), AuditStreamKey()}
				before := snapshotRedisKeysV2(t, client, keys...)
				err := operation.run(ctx, dir, node)
				if err == nil || !strings.Contains(err.Error(), "LEASE_SCORE_MISMATCH") {
					t.Fatalf("%s %s err = %v", status, operation.name, err)
				}
				requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, client, keys...), before)
			})
		}
	}
}

func TestRedisDirectoryReplaceNodeSessionPreflightsBeforeWrite(t *testing.T) {
	ctx := context.Background()
	t.Run("same session", func(t *testing.T) {
		dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
		server.SetTime(time.Unix(800, 0))
		node := testNode("game-1", "session-a")
		if _, err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
		raw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
		score := client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val()
		events := client.XLen(ctx, EventsStreamKey()).Val()
		if _, _, err := dir.ReplaceNodeSession(ctx, node); !errors.Is(err, sp.ErrInvalidNodeSession) {
			t.Fatalf("err = %v", err)
		}
		if client.Get(ctx, NodeKey(node.NodeIdentity)).Val() != raw || client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val() != score || client.XLen(ctx, EventsStreamKey()).Val() != events {
			t.Fatal("same-session replace changed state")
		}
	})
	t.Run("events wrong type", func(t *testing.T) {
		dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
		server.SetTime(time.Unix(820, 0))
		node := testNode("game-1", "session-a")
		if _, err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
		raw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
		score := client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val()
		client.Del(ctx, EventsStreamKey())
		client.Set(ctx, EventsStreamKey(), "wrongtype", 0)
		next := node
		next.NodeSessionID = "session-b"
		if _, _, err := dir.ReplaceNodeSession(ctx, next); err == nil {
			t.Fatal("expected WRONGTYPE")
		}
		if client.Get(ctx, NodeKey(node.NodeIdentity)).Val() != raw || client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val() != score || client.Get(ctx, EventsStreamKey()).Val() != "wrongtype" {
			t.Fatal("WRONGTYPE replace changed state")
		}
	})
}

func TestRedisReplaceMalformedOldJSONLeavesCompleteSnapshotUnchanged(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(810, 0))
	node := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	placement, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "malformed-replace", Kind: "Player", TargetNodeType: node.NodeType, TargetNodeGroup: node.NodeGroup})
	if err != nil {
		t.Fatal(err)
	}
	client.Set(ctx, NodeKey(node.NodeIdentity), "{", 0)
	before := captureRouteMutationSnapshot(t, ctx, client, *placement, node.NodeIdentity)
	next := node
	next.NodeSessionID = "session-b"
	if _, _, err := dir.ReplaceNodeSession(ctx, next); err == nil {
		t.Fatal("expected malformed JSON error")
	}
	requireRouteMutationSnapshot(t, ctx, client, *placement, before, node.NodeIdentity)
}

func TestRedisDirectoryRegisterCannotBypassReplaceSessionEvent(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(830, 0))
	old := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, old); err != nil {
		t.Fatal(err)
	}
	next := old
	next.NodeSessionID = "session-b"
	before := captureLeaseBatchSnapshot(t, ctx, client, old.NodeIdentity)
	if _, err := dir.RegisterNode(ctx, next); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("different-session RegisterNode err = %v", err)
	}
	requireLeaseBatchSnapshot(t, ctx, client, old.NodeIdentity, before)
	returned, _, err := dir.ReplaceNodeSession(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	if returned.NodeSessionID != "session-a" {
		t.Fatalf("old = %+v", returned)
	}
	stored := readNode(t, ctx, dir, next.NodeIdentity)
	if stored.NodeSessionID != next.NodeSessionID || stored.Lease.Version != 1 {
		t.Fatalf("replacement = %+v", stored)
	}
	if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: next.NodeIdentity, NodeSessionID: old.NodeSessionID}); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("old-session RenewNode err = %v", err)
	}
	if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: next.NodeIdentity, NodeSessionID: next.NodeSessionID}); err != nil {
		t.Fatalf("new-session RenewNode err = %v", err)
	}
	events := client.XRange(ctx, EventsStreamKey(), "-", "+").Val()
	replaced := 0
	for _, event := range events {
		if event.Values["type"] == string(sp.EventNodeReplaced) {
			replaced++
		}
	}
	if replaced != 1 {
		t.Fatalf("replace events = %d", replaced)
	}
}

func TestExpireNodeLeasesStaleCandidateDoesNotExpireRenewedLease(t *testing.T) {
	ctx := context.Background()
	base, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(840, 0))
	node := testNode("game-1", "session-a")
	if _, err := base.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.UnixMilli(840500))
	armed := true
	hooked, err := NewDirectory(nodeLeaseEvalHookClient{UniversalClient: client, before: func(script string) {
		if armed && script == expireNodeLeaseLua {
			armed = false
			if _, err := base.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err != nil {
				t.Fatalf("concurrent renew: %v", err)
			}
		}
	}}, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	count, err := hooked.ExpireNodeLeases(ctx, "game", "default", 1)
	if err != nil || count != 0 {
		t.Fatalf("scan = %d, %v", count, err)
	}
	got := readNode(t, ctx, base, node.NodeIdentity)
	if got.Status != sp.NodeStatusActive || got.Lease.Version != 2 || got.Lease.ExpireAtUnixMilli != 841500 {
		t.Fatalf("renewed node = %+v", got)
	}
	for _, event := range client.XRange(ctx, EventsStreamKey(), "-", "+").Val() {
		if event.Values["type"] == string(sp.EventNodeLeaseExpired) {
			t.Fatal("stale scan wrote expiry event")
		}
	}
}

func TestExpireNodeLeasesPreflightsMalformedLaterCandidateBeforeMutation(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(1000, 0))
	first := testNode("game-1", "session-a")
	later := testNode("game-2", "session-b")
	for _, node := range []sp.Node{first, later} {
		if _, err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	client.Set(ctx, NodeKey(later.NodeIdentity), "{", 0)
	server.SetTime(time.Unix(1001, 0))
	before := captureLeaseBatchSnapshot(t, ctx, client, first.NodeIdentity)
	if _, err := dir.ExpireNodeLeases(ctx, "game", "default", 2); err == nil {
		t.Fatal("expected malformed later candidate error")
	}
	requireLeaseBatchSnapshot(t, ctx, client, first.NodeIdentity, before)
}

func TestExpireNodeLeasesPreflightsWrongTypeLaterCandidateBeforeMutation(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(1010, 0))
	first := testNode("game-1", "session-a")
	later := testNode("game-2", "session-b")
	for _, node := range []sp.Node{first, later} {
		if _, err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	client.Del(ctx, NodeKey(later.NodeIdentity))
	client.RPush(ctx, NodeKey(later.NodeIdentity), "wrongtype")
	server.SetTime(time.Unix(1011, 0))
	before := captureLeaseBatchSnapshot(t, ctx, client, first.NodeIdentity)
	if _, err := dir.ExpireNodeLeases(ctx, "game", "default", 2); err == nil {
		t.Fatal("expected WRONGTYPE later candidate error")
	}
	requireLeaseBatchSnapshot(t, ctx, client, first.NodeIdentity, before)
}

func TestRedisDirectoryRenewNodeRejectsUnknownStatusWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(1020, 0))
	node := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	raw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
	var stored redisNode
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatal(err)
	}
	stored.Status = sp.NodeStatus("corrupt")
	changed, _ := json.Marshal(stored)
	client.Set(ctx, NodeKey(node.NodeIdentity), changed, 0)
	before := captureLeaseBatchSnapshot(t, ctx, client, node.NodeIdentity)
	if _, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID}); err == nil {
		t.Fatal("expected unknown status error")
	}
	requireLeaseBatchSnapshot(t, ctx, client, node.NodeIdentity, before)
}

func TestExpireNodeLeasesRejectsUnknownStatusWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(1030, 0))
	node := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	raw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
	var stored redisNode
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatal(err)
	}
	stored.Status = sp.NodeStatus("corrupt")
	changed, _ := json.Marshal(stored)
	client.Set(ctx, NodeKey(node.NodeIdentity), changed, 0)
	server.SetTime(time.Unix(1031, 0))
	before := captureLeaseBatchSnapshot(t, ctx, client, node.NodeIdentity)
	if _, err := dir.ExpireNodeLeases(ctx, "game", "default", 1); err == nil {
		t.Fatal("expected unknown status error")
	}
	requireLeaseBatchSnapshot(t, ctx, client, node.NodeIdentity, before)
}

func TestRedisDirectoryReplaceNodeSessionRejectsOldMetadataMismatch(t *testing.T) {
	for _, field := range []string{"type", "group", "name"} {
		t.Run(field, func(t *testing.T) {
			ctx := context.Background()
			dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
			server.SetTime(time.Unix(1040, 0))
			node := testNode("game-1", "session-a")
			if _, err := dir.RegisterNode(ctx, node); err != nil {
				t.Fatal(err)
			}
			raw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
			var stored redisNode
			if err := json.Unmarshal([]byte(raw), &stored); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "type":
				stored.NodeType = "other"
			case "group":
				stored.NodeGroup = "other"
			case "name":
				stored.NodeName = "other"
			}
			changed, _ := json.Marshal(stored)
			client.Set(ctx, NodeKey(node.NodeIdentity), changed, 0)
			before := captureLeaseBatchSnapshot(t, ctx, client, node.NodeIdentity)
			next := node
			next.NodeSessionID = "session-b"
			if _, _, err := dir.ReplaceNodeSession(ctx, next); err == nil {
				t.Fatal("expected old metadata mismatch error")
			}
			requireLeaseBatchSnapshot(t, ctx, client, node.NodeIdentity, before)
		})
	}
}
