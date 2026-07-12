package redis

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func snapshotRedisKeysV2(t *testing.T, client goredis.UniversalClient, keys ...string) map[string]string {
	t.Helper()
	ctx := context.Background()
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		typ, err := client.Type(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		var value any
		switch typ {
		case "none":
			value = nil
		case "string":
			value, err = client.Get(ctx, key).Result()
		case "set":
			var members []string
			members, err = client.SMembers(ctx, key).Result()
			sort.Strings(members)
			value = members
		case "zset":
			value, err = client.ZRangeWithScores(ctx, key, 0, -1).Result()
		case "stream":
			value, err = client.XRange(ctx, key, "-", "+").Result()
		case "list":
			value, err = client.LRange(ctx, key, 0, -1).Result()
		default:
			t.Fatalf("unsupported type %q", typ)
		}
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(struct {
			Type  string
			Value any
		}{typ, value})
		result[key] = string(raw)
	}
	return result
}
func requireRedisSnapshotV2(t *testing.T, got, want map[string]string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("redis state changed\ngot: %#v\nwant:%#v", got, want)
	}
}

type atomicFixture struct {
	dir        *Directory
	client     *goredis.Client
	serverTime int64
	a, b       sp.Node
	p          *sp.Placement
	keys       []string
}

func newAtomicFixture(t *testing.T, grain string) atomicFixture {
	t.Helper()
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(1100, 0))
	a, b := testNode("game-1", "session-a"), testNode("game-2", "session-b")
	for _, node := range []sp.Node{a, b} {
		if err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	p, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: grain, Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{PlacementKey(p.GrainKey), NodeKey(a.NodeIdentity), NodeKey(b.NodeIdentity), NodesKey("game", "default"), NodeLeaseKey("game", "default"), PlacementNodeKey(a.NodeIdentity), PlacementNodeKey(b.NodeIdentity), InvalidNodesKey("game", "default"), SequenceKey(), StrategyRoundRobinKey("game", "default"), EventsStreamKey(), AuditStreamKey()}
	return atomicFixture{dir: dir, client: client, serverTime: 1100, a: a, b: b, p: p, keys: keys}
}
func (f atomicFixture) target() sp.Node {
	if f.p.NodeIdentity == f.a.NodeIdentity {
		return f.b
	}
	return f.a
}
func setOwnerUnavailable(t *testing.T, f atomicFixture) {
	t.Helper()
	ctx := context.Background()
	var owner redisNode
	if err := json.Unmarshal([]byte(f.client.Get(ctx, NodeKey(f.p.NodeIdentity)).Val()), &owner); err != nil {
		t.Fatal(err)
	}
	owner.Status = sp.NodeStatusOffline
	raw, _ := json.Marshal(owner)
	f.client.Set(ctx, NodeKey(f.p.NodeIdentity), raw, 0)
}

func TestRedisDirectoryMutationWrongTypeV2IsAtomic(t *testing.T) {
	cases := []struct{ name, operation, corrupt string }{
		{"allocate events", "allocate", "events"}, {"allocate candidate index", "allocate", "target_index"},
		{"release events", "release", "events"}, {"release old index", "release", "old_index"}, {"release sequence", "release", "sequence"},
		{"transfer events", "transfer", "events"}, {"transfer target index", "transfer", "target_index"}, {"transfer invalid set", "transfer", "invalid"}, {"transfer sequence", "transfer", "sequence"},
		{"recover events", "recover", "events"}, {"recover target index", "recover", "target_index"}, {"recover invalid set", "recover", "invalid"}, {"recover sequence", "recover", "sequence"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newAtomicFixture(t, "wrongtype-"+strings.ReplaceAll(tc.name, " ", "-"))
			target := f.target()
			if tc.operation == "recover" {
				setOwnerUnavailable(t, f)
			}
			key := ""
			switch tc.corrupt {
			case "events":
				key = EventsStreamKey()
			case "target_index":
				key = PlacementNodeKey(target.NodeIdentity)
			case "old_index":
				key = PlacementNodeKey(f.p.NodeIdentity)
			case "invalid":
				key = InvalidNodesKey("game", "default")
			case "sequence":
				key = SequenceKey()
			}
			f.client.Del(ctx, key)
			if tc.corrupt == "sequence" {
				f.client.RPush(ctx, key, "wrongtype")
			} else {
				f.client.Set(ctx, key, "wrongtype", 0)
			}
			before := snapshotRedisKeysV2(t, f.client, f.keys...)
			var err error
			switch tc.operation {
			case "allocate":
				_, err = f.dir.Allocate(ctx, sp.AllocateCommand{GrainID: "new-" + tc.corrupt, Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			case "release":
				err = f.dir.Release(ctx, sp.ReleaseCommand{GrainKey: f.p.GrainKey, NodeIdentity: f.p.NodeIdentity, NodeSessionID: f.p.OwnerNodeSessionID, PlacementVersion: f.p.Version})
			case "transfer":
				_, err = f.dir.Transfer(ctx, sp.TransferCommand{GrainKey: f.p.GrainKey, FromNodeIdentity: f.p.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
			case "recover":
				_, err = f.dir.Recover(ctx, sp.RecoverCommand{GrainKey: f.p.GrainKey, NewNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
			}
			if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
				t.Fatalf("err=%v", err)
			}
			requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, f.client, f.keys...), before)
		})
	}
}

func corruptWrongTypeV2(t *testing.T, ctx context.Context, client *goredis.Client, key, expected string) {
	t.Helper()
	client.Del(ctx, key)
	if expected == "string" {
		client.RPush(ctx, key, "wrongtype")
	} else {
		client.Set(ctx, key, "wrongtype", 0)
	}
}

func TestRedisDirectoryAllocateWrongTypeKeyMatrixV2(t *testing.T) {
	for _, tc := range []struct {
		name, expected string
		key            func(sp.GrainKey, sp.Node) string
	}{{"placement", "string", func(k sp.GrainKey, _ sp.Node) string { return PlacementKey(k) }}, {"nodes", "set", func(_ sp.GrainKey, _ sp.Node) string { return NodesKey("game", "default") }}, {"invalid", "set", func(_ sp.GrainKey, _ sp.Node) string { return InvalidNodesKey("game", "default") }}, {"round robin", "string", func(_ sp.GrainKey, _ sp.Node) string { return StrategyRoundRobinKey("game", "default") }}, {"leases", "zset", func(_ sp.GrainKey, _ sp.Node) string { return NodeLeaseKey("game", "default") }}, {"sequence", "string", func(_ sp.GrainKey, _ sp.Node) string { return SequenceKey() }}, {"events", "stream", func(_ sp.GrainKey, _ sp.Node) string { return EventsStreamKey() }}, {"candidate node", "string", func(_ sp.GrainKey, n sp.Node) string { return NodeKey(n.NodeIdentity) }}, {"candidate index", "zset", func(_ sp.GrainKey, n sp.Node) string { return PlacementNodeKey(n.NodeIdentity) }}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
			server.SetTime(time.Unix(1220, 0))
			node := testNode("game-1", "session-a")
			if err := dir.RegisterNode(ctx, node); err != nil {
				t.Fatal(err)
			}
			grain, _ := sp.NewGrainKey("Player", "allocate-matrix-"+strings.ReplaceAll(tc.name, " ", "-"))
			key := tc.key(grain, node)
			corruptWrongTypeV2(t, ctx, client, key, tc.expected)
			keys := []string{PlacementKey(grain), NodesKey("game", "default"), InvalidNodesKey("game", "default"), StrategyRoundRobinKey("game", "default"), NodeLeaseKey("game", "default"), SequenceKey(), EventsStreamKey(), NodeKey(node.NodeIdentity), PlacementNodeKey(node.NodeIdentity)}
			before := snapshotRedisKeysV2(t, client, keys...)
			_, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: strings.TrimPrefix(grain.String(), "Player/"), Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
				t.Fatalf("err=%v", err)
			}
			requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, client, keys...), before)
		})
	}
}

func TestRedisDirectoryAllocateExistingOwnerWrongTypeKeyMatrixV2(t *testing.T) {
	for _, tc := range []struct{ name, part, expected string }{{"old index", "old_index", "zset"}, {"old node", "old_node", "string"}, {"owner lease", "owner_lease", "zset"}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newAtomicFixture(t, "allocate-existing-"+strings.ReplaceAll(tc.name, " ", "-"))
			key := ""
			switch tc.part {
			case "old_index":
				key = PlacementNodeKey(f.p.NodeIdentity)
			case "old_node":
				key = NodeKey(f.p.NodeIdentity)
			case "owner_lease":
				key = NodeLeaseKey("game", "default")
			}
			corruptWrongTypeV2(t, ctx, f.client, key, tc.expected)
			before := snapshotRedisKeysV2(t, f.client, f.keys...)
			_, err := f.dir.Allocate(ctx, sp.AllocateCommand{GrainID: f.p.GrainID, Kind: f.p.Kind, TargetNodeType: "game", TargetNodeGroup: "default"})
			if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
				t.Fatalf("err=%v", err)
			}
			requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, f.client, f.keys...), before)
		})
	}
}

func TestRedisDirectoryPlacementMutationWrongTypeKeyMatrixV2(t *testing.T) {
	cases := []struct{ name, operation, part, expected string }{
		{"release placement", "release", "placement", "string"}, {"release owner", "release", "owner", "string"}, {"release old index", "release", "old_index", "zset"}, {"release owner lease", "release", "owner_lease", "zset"}, {"release events", "release", "events", "stream"}, {"release sequence", "release", "sequence", "string"},
		{"transfer placement", "transfer", "placement", "string"}, {"transfer owner", "transfer", "owner", "string"}, {"transfer target", "transfer", "target", "string"}, {"transfer old index", "transfer", "old_index", "zset"}, {"transfer target index", "transfer", "target_index", "zset"}, {"transfer target lease", "transfer", "target_lease", "zset"}, {"transfer events", "transfer", "events", "stream"}, {"transfer owner lease", "transfer", "owner_lease", "zset"}, {"transfer invalid", "transfer", "invalid", "set"}, {"transfer sequence", "transfer", "sequence", "string"},
		{"recover placement", "recover", "placement", "string"}, {"recover owner", "recover", "owner", "string"}, {"recover target", "recover", "target", "string"}, {"recover old index", "recover", "old_index", "zset"}, {"recover target index", "recover", "target_index", "zset"}, {"recover target lease", "recover", "target_lease", "zset"}, {"recover events", "recover", "events", "stream"}, {"recover owner lease", "recover", "owner_lease", "zset"}, {"recover invalid", "recover", "invalid", "set"}, {"recover sequence", "recover", "sequence", "string"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newAtomicFixture(t, "matrix-"+strings.ReplaceAll(tc.name, " ", "-"))
			target := f.target()
			if tc.operation == "recover" {
				setOwnerUnavailable(t, f)
			}
			key := ""
			switch tc.part {
			case "placement":
				key = PlacementKey(f.p.GrainKey)
			case "owner":
				key = NodeKey(f.p.NodeIdentity)
			case "target":
				key = NodeKey(target.NodeIdentity)
			case "old_index":
				key = PlacementNodeKey(f.p.NodeIdentity)
			case "target_index":
				key = PlacementNodeKey(target.NodeIdentity)
			case "target_lease", "owner_lease":
				key = NodeLeaseKey("game", "default")
			case "events":
				key = EventsStreamKey()
			case "invalid":
				key = InvalidNodesKey("game", "default")
			case "sequence":
				key = SequenceKey()
			}
			corruptWrongTypeV2(t, ctx, f.client, key, tc.expected)
			before := snapshotRedisKeysV2(t, f.client, f.keys...)
			var err error
			if tc.operation == "release" {
				err = f.dir.Release(ctx, sp.ReleaseCommand{GrainKey: f.p.GrainKey, NodeIdentity: f.p.NodeIdentity, NodeSessionID: f.p.OwnerNodeSessionID, PlacementVersion: f.p.Version})
			} else if tc.operation == "transfer" {
				_, err = f.dir.Transfer(ctx, sp.TransferCommand{GrainKey: f.p.GrainKey, FromNodeIdentity: f.p.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
			} else {
				_, err = f.dir.Recover(ctx, sp.RecoverCommand{GrainKey: f.p.GrainKey, NewNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
			}
			if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
				t.Fatalf("err=%v", err)
			}
			requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, f.client, f.keys...), before)
		})
	}
}

func TestRedisDirectoryMalformedJSONV2IsAtomic(t *testing.T) {
	for _, tc := range []struct{ name, operation, part string }{{"allocate candidate", "allocate", "target"}, {"release placement", "release", "placement"}, {"release owner", "release", "owner"}, {"transfer owner", "transfer", "owner"}, {"transfer target", "transfer", "target"}, {"recover placement", "recover", "placement"}, {"recover owner", "recover", "owner"}, {"recover target", "recover", "target"}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newAtomicFixture(t, "malformed-"+strings.ReplaceAll(tc.name, " ", "-"))
			target := f.target()
			if tc.operation == "recover" {
				setOwnerUnavailable(t, f)
			}
			var key string
			switch tc.part {
			case "placement":
				key = PlacementKey(f.p.GrainKey)
			case "owner":
				key = NodeKey(f.p.NodeIdentity)
			case "target":
				key = NodeKey(target.NodeIdentity)
			}
			f.client.Set(ctx, key, "{", 0)
			before := snapshotRedisKeysV2(t, f.client, f.keys...)
			var err error
			switch tc.operation {
			case "allocate":
				_, err = f.dir.Allocate(ctx, sp.AllocateCommand{GrainID: "new-malformed", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			case "release":
				err = f.dir.Release(ctx, sp.ReleaseCommand{GrainKey: f.p.GrainKey, NodeIdentity: f.p.NodeIdentity, NodeSessionID: f.p.OwnerNodeSessionID, PlacementVersion: f.p.Version})
			case "transfer":
				_, err = f.dir.Transfer(ctx, sp.TransferCommand{GrainKey: f.p.GrainKey, FromNodeIdentity: f.p.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
			case "recover":
				_, err = f.dir.Recover(ctx, sp.RecoverCommand{GrainKey: f.p.GrainKey, NewNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
			}
			if err == nil {
				t.Fatal("expected malformed JSON error")
			}
			requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, f.client, f.keys...), before)
		})
	}
}

func TestRedisDirectoryAllocateCounterBoundariesV2AreAtomic(t *testing.T) {
	for _, counter := range []struct{ name, key, value string }{{"round robin leading zeros", StrategyRoundRobinKey("game", "default"), "00"}, {"round robin max", StrategyRoundRobinKey("game", "default"), "9223372036854775807"}, {"sequence leading zeros", SequenceKey(), "00"}, {"sequence max", SequenceKey(), "9007199254740991"}} {
		t.Run(counter.name, func(t *testing.T) {
			ctx := context.Background()
			f := newAtomicFixture(t, "counter-"+strings.ReplaceAll(counter.name, " ", "-"))
			f.client.Set(ctx, counter.key, counter.value, 0)
			before := snapshotRedisKeysV2(t, f.client, f.keys...)
			_, err := f.dir.Allocate(ctx, sp.AllocateCommand{GrainID: "counter-new", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			if err == nil || !strings.Contains(err.Error(), "INVALID_COUNTER") {
				t.Fatalf("err=%v", err)
			}
			requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, f.client, f.keys...), before)
		})
	}
}

func TestRedisDirectoryRenewReleaseEscapedSessionExactV2(t *testing.T) {
	for _, operation := range []string{"renew", "release"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			dir, _, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
			server.SetTime(time.Unix(1200, 0))
			node := testNode("game-1", `session\"with\\escapes`)
			if err := dir.RegisterNode(ctx, node); err != nil {
				t.Fatal(err)
			}
			p, err := dir.Allocate(ctx, sp.AllocateCommand{GrainID: "escaped-" + operation, Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			if err != nil {
				t.Fatal(err)
			}
			if operation == "renew" {
				_, err = dir.Renew(ctx, sp.RenewCommand{GrainKey: p.GrainKey, NodeIdentity: p.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: p.Version})
			} else {
				err = dir.Release(ctx, sp.ReleaseCommand{GrainKey: p.GrainKey, NodeIdentity: p.NodeIdentity, NodeSessionID: node.NodeSessionID, PlacementVersion: p.Version})
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRedisDirectoryRealRedisLuaAtomicityV2(t *testing.T) {
	addr := os.Getenv("STABLE_PLACEMENT_REAL_REDIS_ADDR")
	if addr == "" {
		t.Skip("STABLE_PLACEMENT_REAL_REDIS_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("STABLE_PLACEMENT_REAL_REDIS_PASSWORD")})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	tag := "{node-lease-v2-atomic}"
	key := func(s string) string { return "sp:" + tag + ":" + s }
	keys := []string{key("placement"), key("owner"), key("target"), key("old-index"), key("new-index"), key("target-leases"), key("events"), key("owner-leases"), key("invalid"), key("sequence")}
	t.Cleanup(func() { client.Del(context.Background(), keys...) })
	client.Set(ctx, keys[0], `{"GrainKey":"Player/1","NodeIdentity":"old","OwnerNodeSessionID":"s1","Version":1,"Status":"active"}`, 0)
	client.Set(ctx, keys[6], "wrongtype", 0)
	before := snapshotRedisKeysV2(t, client, keys...)
	err := client.Eval(ctx, mutationLua, keys, "release", client.Get(ctx, keys[0]).Val(), "old", "", "s1", "1", string(sp.EventPlacementReleased), "Player/1", "old", "group", "name", "", "", "").Err()
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("err=%v", err)
	}
	requireRedisSnapshotV2(t, snapshotRedisKeysV2(t, client, keys...), before)
}

func TestRedisDirectoryReleaseRejectsSessionReplacedBeforeEvalV2(t *testing.T) {
	ctx := context.Background()
	base, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(1210, 0))
	node := testNode("game-1", "session-a")
	if err := base.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	p, err := base.Allocate(ctx, sp.AllocateCommand{GrainID: "release-replaced", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	beforePlacement := client.Get(ctx, PlacementKey(p.GrainKey)).Val()
	beforeIndex := client.ZRangeWithScores(ctx, PlacementNodeKey(p.NodeIdentity), 0, -1).Val()
	beforeSequence := client.Get(ctx, SequenceKey()).Val()
	armed := true
	hooked, err := NewDirectory(nodeLeaseEvalHookClient{UniversalClient: client, before: func(script string) {
		if armed && script == mutationLua {
			armed = false
			next := node
			next.NodeSessionID = "session-b"
			if _, err := base.ReplaceNodeSession(ctx, next); err != nil {
				t.Fatal(err)
			}
		}
	}}, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = hooked.Release(ctx, sp.ReleaseCommand{GrainKey: p.GrainKey, NodeIdentity: p.NodeIdentity, NodeSessionID: p.OwnerNodeSessionID, PlacementVersion: p.Version})
	if !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("err=%v", err)
	}
	if client.Get(ctx, PlacementKey(p.GrainKey)).Val() != beforePlacement || !reflect.DeepEqual(client.ZRangeWithScores(ctx, PlacementNodeKey(p.NodeIdentity), 0, -1).Val(), beforeIndex) || client.Get(ctx, SequenceKey()).Val() != beforeSequence {
		t.Fatal("release committed after session replacement")
	}
}

func TestRedisDirectoryTransferTargetInvalidatedBeforeEvalV2(t *testing.T) {
	ctx := context.Background()
	f := newAtomicFixture(t, "target-invalidated")
	target := f.target()
	beforePlacement := f.client.Get(ctx, PlacementKey(f.p.GrainKey)).Val()
	beforeOld := f.client.ZRangeWithScores(ctx, PlacementNodeKey(f.p.NodeIdentity), 0, -1).Val()
	beforeSequence := f.client.Get(ctx, SequenceKey()).Val()
	armed := true
	hooked, err := NewDirectory(nodeLeaseEvalHookClient{UniversalClient: f.client, before: func(script string) {
		if armed && script == mutationLua {
			armed = false
			if err := f.dir.MarkNodeInvalid(ctx, target.NodeType, target.NodeGroup, target.NodeName); err != nil {
				t.Fatal(err)
			}
		}
	}}, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = hooked.Transfer(ctx, sp.TransferCommand{GrainKey: f.p.GrainKey, FromNodeIdentity: f.p.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
	if !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("err=%v", err)
	}
	if f.client.Get(ctx, PlacementKey(f.p.GrainKey)).Val() != beforePlacement || !reflect.DeepEqual(f.client.ZRangeWithScores(ctx, PlacementNodeKey(f.p.NodeIdentity), 0, -1).Val(), beforeOld) || f.client.Get(ctx, SequenceKey()).Val() != beforeSequence {
		t.Fatal("transfer committed after target invalidation")
	}
}

func TestRedisDirectoryRecoverTargetUnregisteredBeforeEvalV2(t *testing.T) {
	ctx := context.Background()
	f := newAtomicFixture(t, "target-unregistered")
	target := f.target()
	setOwnerUnavailable(t, f)
	beforePlacement := f.client.Get(ctx, PlacementKey(f.p.GrainKey)).Val()
	beforeOld := f.client.ZRangeWithScores(ctx, PlacementNodeKey(f.p.NodeIdentity), 0, -1).Val()
	beforeSequence := f.client.Get(ctx, SequenceKey()).Val()
	armed := true
	hooked, err := NewDirectory(nodeLeaseEvalHookClient{UniversalClient: f.client, before: func(script string) {
		if armed && script == mutationLua {
			armed = false
			if err := f.dir.UnregisterNode(ctx, target.NodeIdentity, target.NodeSessionID); err != nil {
				t.Fatal(err)
			}
		}
	}}, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = hooked.Recover(ctx, sp.RecoverCommand{GrainKey: f.p.GrainKey, NewNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
	if !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("err=%v", err)
	}
	if f.client.Get(ctx, PlacementKey(f.p.GrainKey)).Val() != beforePlacement || !reflect.DeepEqual(f.client.ZRangeWithScores(ctx, PlacementNodeKey(f.p.NodeIdentity), 0, -1).Val(), beforeOld) || f.client.Get(ctx, SequenceKey()).Val() != beforeSequence {
		t.Fatal("recover committed after target unregister")
	}
}

func TestRedisDirectoryTransferRecoverUsesTargetSessionAtEvalV2(t *testing.T) {
	for _, operation := range []string{"transfer", "recover"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			f := newAtomicFixture(t, "target-session-"+operation)
			target := f.target()
			if operation == "recover" {
				setOwnerUnavailable(t, f)
			}
			armed := true
			hooked, err := NewDirectory(nodeLeaseEvalHookClient{UniversalClient: f.client, before: func(script string) {
				if armed && script == mutationLua {
					armed = false
					next := target
					next.NodeSessionID = "session-c"
					if _, err := f.dir.ReplaceNodeSession(ctx, next); err != nil {
						t.Fatal(err)
					}
				}
			}}, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			var updated *sp.Placement
			if operation == "transfer" {
				updated, err = hooked.Transfer(ctx, sp.TransferCommand{GrainKey: f.p.GrainKey, FromNodeIdentity: f.p.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
			} else {
				updated, err = hooked.Recover(ctx, sp.RecoverCommand{GrainKey: f.p.GrainKey, NewNodeIdentity: target.NodeIdentity, PlacementVersion: f.p.Version})
			}
			if err != nil {
				t.Fatal(err)
			}
			if updated.OwnerNodeSessionID != "session-c" {
				t.Fatalf("owner session=%q", updated.OwnerNodeSessionID)
			}
		})
	}
}
