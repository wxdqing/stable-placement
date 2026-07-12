package redis

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func TestRedisDirectoryExpireHeartbeatsMarksOffline(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	dir.SetHeartbeatTTL(time.Second)

	staleAt := time.Now().Add(-10 * time.Second)
	stale := registerHeartbeatTestNode(t, dir, "game-1", "session-a", staleAt)
	fresh := registerHeartbeatTestNode(t, dir, "game-2", "session-b", staleAt)
	if err := dir.RenewNode(ctx, fresh.NodeIdentity, fresh.NodeSessionID); err != nil {
		t.Fatalf("RenewNode fresh error: %v", err)
	}

	beforeEvents := client.XLen(ctx, EventsStreamKey()).Val()
	count, err := dir.ExpireHeartbeats(ctx, stale.NodeType, stale.NodeGroup, staleAt.Add(2*time.Second), 10)
	if err != nil {
		t.Fatalf("ExpireHeartbeats error: %v", err)
	}
	if count != 1 {
		t.Fatalf("ExpireHeartbeats count = %d, want 1", count)
	}

	nodes, err := dir.FindNodes(ctx, stale.NodeType, stale.NodeGroup)
	if err != nil {
		t.Fatalf("FindNodes error: %v", err)
	}
	if len(nodes) != 2 || nodes[0].Status != sp.NodeStatusOffline || nodes[1].Status != sp.NodeStatusActive {
		t.Fatalf("nodes after heartbeat expiry = %+v", nodes)
	}
	if events := client.XRange(ctx, EventsStreamKey(), "-", "+").Val(); len(events) != int(beforeEvents)+1 || events[len(events)-1].Values["type"] != string(sp.EventNodeUnregistered) {
		t.Fatalf("events after heartbeat expiry = %+v", events)
	}

	placement, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  stale.NodeType,
		TargetNodeGroup: stale.NodeGroup,
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	if placement.NodeIdentity != fresh.NodeIdentity {
		t.Fatalf("allocated node = %q, want %q", placement.NodeIdentity, fresh.NodeIdentity)
	}
}

func TestRedisDirectoryHeartbeatIndexTracksNodeLifecycle(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	registeredAt := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	node := registerHeartbeatTestNode(t, dir, "game-1", "session-a", registeredAt)
	heartbeatKey := NodeHeartbeatKey(node.NodeType, node.NodeGroup)
	nodeKey := NodeKey(node.NodeIdentity)
	if score := client.ZScore(ctx, heartbeatKey, nodeKey).Val(); score != float64(registeredAt.UnixMilli()) {
		t.Fatalf("registered heartbeat score = %v, want %d", score, registeredAt.UnixMilli())
	}

	if err := dir.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatalf("RenewNode error: %v", err)
	}
	renewed, err := dir.getNode(ctx, node.NodeIdentity)
	if err != nil {
		t.Fatalf("get renewed node error: %v", err)
	}
	if score := client.ZScore(ctx, heartbeatKey, nodeKey).Val(); score != float64(renewed.LastHeartbeatAt.UnixMilli()) {
		t.Fatalf("renewed heartbeat score = %v, want %d", score, renewed.LastHeartbeatAt.UnixMilli())
	}

	replacedAt := renewed.LastHeartbeatAt.Add(time.Second)
	replacement := *renewed
	replacement.NodeSessionID = "session-b"
	replacement.LastHeartbeatAt = replacedAt
	if _, err := dir.ReplaceNodeSession(ctx, replacement); err != nil {
		t.Fatalf("ReplaceNodeSession error: %v", err)
	}
	if score := client.ZScore(ctx, heartbeatKey, nodeKey).Val(); score != float64(replacedAt.UnixMilli()) {
		t.Fatalf("replaced heartbeat score = %v, want %d", score, replacedAt.UnixMilli())
	}

	if err := dir.UnregisterNode(ctx, node.NodeIdentity, replacement.NodeSessionID); err != nil {
		t.Fatalf("UnregisterNode error: %v", err)
	}
	if _, err := client.ZScore(ctx, heartbeatKey, nodeKey).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("heartbeat after unregister error = %v, want redis.Nil", err)
	}
}

func TestRedisDirectoryExpireHeartbeatsWritesOfflineEventOnce(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	dir.SetHeartbeatTTL(time.Second)
	staleAt := time.Now().Add(-time.Minute)
	node := registerHeartbeatTestNode(t, dir, "game-1", "session-a", staleAt)
	now := staleAt.Add(2 * time.Second)

	for scan := 0; scan < 2; scan++ {
		count, err := dir.ExpireHeartbeats(ctx, node.NodeType, node.NodeGroup, now, 10)
		if err != nil {
			t.Fatalf("ExpireHeartbeats scan %d error: %v", scan, err)
		}
		if want := 1 - scan; count != want {
			t.Fatalf("ExpireHeartbeats scan %d count = %d, want %d", scan, count, want)
		}
	}
	events := client.XRange(ctx, EventsStreamKey(), "-", "+").Val()
	if len(events) != 2 || events[1].Values["type"] != string(sp.EventNodeUnregistered) {
		t.Fatalf("events after repeated expiry = %+v", events)
	}
}

func TestRedisDirectoryExpireHeartbeatsPaginatesEqualScoresPastStaleMembers(t *testing.T) {
	ctx := context.Background()
	dir, base := newTestDirectory(t)
	dir.SetHeartbeatTTL(time.Second)
	heartbeatKey := NodeHeartbeatKey("game", "default")
	now := time.Now().Truncate(time.Millisecond)
	sharedScore := now.Add(-2 * time.Second).UnixMilli()
	members := []string{
		NodeKey("game/default/game-a"),
		NodeKey("game/default/game-b"),
		NodeKey("game/default/game-c"),
		NodeKey("game/default/game-d"),
	}
	sort.Strings(members)
	writeNode := func(member string, node sp.Node) {
		t.Helper()
		raw, err := marshalRedisNode(node)
		if err != nil {
			t.Fatal(err)
		}
		if err := base.Set(ctx, member, raw, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	writeNode(members[0], sp.Node{
		NodeType: "game", NodeGroup: "default", NodeName: "draining",
		NodeIdentity: "game/default/draining", NodeSessionID: "session-a", Status: sp.NodeStatusDraining,
	})
	// members[1] intentionally has no raw node.
	writeNode(members[2], sp.Node{
		NodeType: "game", NodeGroup: "other", NodeName: "wrong-group",
		NodeIdentity: "game/other/wrong-group", NodeSessionID: "session-c", Status: sp.NodeStatusActive,
	})
	active := sp.Node{
		NodeType: "game", NodeGroup: "default", NodeName: "active",
		NodeIdentity: "game/default/active", NodeSessionID: "session-d", Status: sp.NodeStatusActive,
		LastHeartbeatAt: time.UnixMilli(sharedScore),
	}
	writeNode(members[3], active)
	for _, member := range members {
		if err := base.ZAdd(ctx, heartbeatKey, goredis.Z{Score: float64(sharedScore), Member: member}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	recording := &zRangeCountClient{UniversalClient: base}
	dir.client = recording

	count, err := dir.ExpireHeartbeats(ctx, "game", "default", now, 1)
	if err != nil {
		t.Fatalf("ExpireHeartbeats error: %v", err)
	}
	if count != 1 {
		t.Fatalf("ExpireHeartbeats count = %d, want 1", count)
	}
	counts := recording.Counts()
	if len(counts) < len(members) || len(counts) > maxExpireScanRounds {
		t.Fatalf("query rounds = %d, want [%d, %d]; counts = %v", len(counts), len(members), maxExpireScanRounds, counts)
	}
	for _, queryCount := range counts {
		if queryCount > 1 {
			t.Fatalf("query Count = %d, want <= 1; all counts = %v", queryCount, counts)
		}
	}
	if remaining := base.ZCard(ctx, heartbeatKey).Val(); remaining != 0 {
		t.Fatalf("heartbeat members remaining = %d, want 0", remaining)
	}
	raw, err := base.Get(ctx, members[3]).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var stored sp.Node
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status != sp.NodeStatusOffline {
		t.Fatalf("active node status = %s, want offline", stored.Status)
	}
	if events := base.XLen(ctx, EventsStreamKey()).Val(); events != 1 {
		t.Fatalf("offline events = %d, want 1", events)
	}
}

func TestRedisDirectoryExpireHeartbeatsDoesNotExpireConcurrentRenew(t *testing.T) {
	ctx := context.Background()
	dir, base := newTestDirectory(t)
	dir.SetHeartbeatTTL(time.Second)
	staleAt := time.Now().Add(-time.Minute)
	node := registerHeartbeatTestNode(t, dir, "game-1", "session-a", staleAt)

	var once sync.Once
	hooked, err := NewDirectory(evalHookClient{
		UniversalClient: base,
		beforeEval: func(script string) {
			if script != expireHeartbeatLua {
				return
			}
			once.Do(func() {
				if err := dir.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
					t.Fatalf("concurrent RenewNode error: %v", err)
				}
			})
		},
	}, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	hooked.SetHeartbeatTTL(time.Second)

	count, err := hooked.ExpireHeartbeats(ctx, node.NodeType, node.NodeGroup, staleAt.Add(2*time.Second), 1)
	if err != nil {
		t.Fatalf("ExpireHeartbeats error: %v", err)
	}
	if count != 0 {
		t.Fatalf("ExpireHeartbeats count = %d, want 0", count)
	}
	stored, err := dir.getNode(ctx, node.NodeIdentity)
	if err != nil {
		t.Fatalf("getNode error: %v", err)
	}
	if stored.Status != sp.NodeStatusActive {
		t.Fatalf("node status = %s, want active", stored.Status)
	}
	if events := base.XLen(ctx, EventsStreamKey()).Val(); events != 1 {
		t.Fatalf("events after concurrent renew = %d, want 1", events)
	}
}

func TestRedisDirectoryExpireHeartbeatsWrongTypeKeepsState(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	dir.SetHeartbeatTTL(time.Second)
	staleAt := time.Now().Add(-time.Minute)
	node := registerHeartbeatTestNode(t, dir, "game-1", "session-a", staleAt)
	if err := client.Del(ctx, EventsStreamKey()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, EventsStreamKey(), "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := dir.ExpireHeartbeats(ctx, node.NodeType, node.NodeGroup, staleAt.Add(2*time.Second), 1); err == nil {
		t.Fatal("ExpireHeartbeats error = nil, want WRONGTYPE")
	}
	stored, err := dir.getNode(ctx, node.NodeIdentity)
	if err != nil {
		t.Fatalf("getNode error: %v", err)
	}
	if stored.Status != sp.NodeStatusActive {
		t.Fatalf("node status = %s, want active", stored.Status)
	}
	if _, err := client.ZScore(ctx, NodeHeartbeatKey(node.NodeType, node.NodeGroup), NodeKey(node.NodeIdentity)).Result(); err != nil {
		t.Fatalf("heartbeat removed after rejected expiry: %v", err)
	}
}

func TestRedisDirectoryRegisterHeartbeatWrongTypeKeepsState(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	heartbeatKey := NodeHeartbeatKey("game", "default")
	if err := client.Set(ctx, heartbeatKey, "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	node := sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      "game-1",
		NodeSessionID: "session-a",
	}
	if err := dir.RegisterNode(ctx, node); err == nil {
		t.Fatal("RegisterNode error = nil, want WRONGTYPE")
	}
	identity := "game/default/game-1"
	if exists := client.Exists(ctx, NodeKey(identity), NodesKey(node.NodeType, node.NodeGroup), EventsStreamKey()).Val(); exists != 0 {
		t.Fatalf("node state exists after rejected register: %d keys", exists)
	}
}

func TestRedisDirectoryRegisterRejectsNodeIdentityMetadataMismatchWithoutWrites(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	node := sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      "game-1",
		NodeIdentity:  "game/other/game-1",
		NodeSessionID: "session-a",
	}

	if err := dir.RegisterNode(ctx, node); err == nil {
		t.Fatal("RegisterNode error = nil, want identity metadata mismatch")
	}
	keys := []string{
		NodeKey(node.NodeIdentity),
		NodeKey("game/default/game-1"),
		NodesKey(node.NodeType, node.NodeGroup),
		NodeHeartbeatKey(node.NodeType, node.NodeGroup),
		EventsStreamKey(),
	}
	if exists := client.Exists(ctx, keys...).Val(); exists != 0 {
		t.Fatalf("state exists after rejected register: %d keys", exists)
	}
}

func TestRedisDirectoryReplaceRejectsNodeIdentityMetadataMismatchWithoutWrites(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	node := registerTestNode(t, dir, "game-1", "session-a")
	nodeKey := NodeKey(node.NodeIdentity)
	heartbeatKey := NodeHeartbeatKey(node.NodeType, node.NodeGroup)
	rawBefore := client.Get(ctx, nodeKey).Val()
	membersBefore := client.SMembers(ctx, NodesKey(node.NodeType, node.NodeGroup)).Val()
	heartbeatBefore := client.ZScore(ctx, heartbeatKey, nodeKey).Val()
	eventsBefore := client.XLen(ctx, EventsStreamKey()).Val()

	replacement := node
	replacement.NodeGroup = "other"
	replacement.NodeSessionID = "session-b"
	if _, err := dir.ReplaceNodeSession(ctx, replacement); err == nil {
		t.Fatal("ReplaceNodeSession error = nil, want identity metadata mismatch")
	}
	if raw := client.Get(ctx, nodeKey).Val(); raw != rawBefore {
		t.Fatalf("raw changed after rejected replace: got %q, want %q", raw, rawBefore)
	}
	if members := client.SMembers(ctx, NodesKey(node.NodeType, node.NodeGroup)).Val(); !sameStrings(members, membersBefore) {
		t.Fatalf("nodes changed after rejected replace: got %v, want %v", members, membersBefore)
	}
	if score := client.ZScore(ctx, heartbeatKey, nodeKey).Val(); score != heartbeatBefore {
		t.Fatalf("heartbeat changed after rejected replace: got %v, want %v", score, heartbeatBefore)
	}
	if events := client.XLen(ctx, EventsStreamKey()).Val(); events != eventsBefore {
		t.Fatalf("events changed after rejected replace: got %d, want %d", events, eventsBefore)
	}
	if exists := client.Exists(ctx,
		NodesKey(replacement.NodeType, replacement.NodeGroup),
		NodeHeartbeatKey(replacement.NodeType, replacement.NodeGroup),
	).Val(); exists != 0 {
		t.Fatalf("replacement group state exists after rejection: %d keys", exists)
	}
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	want := make(map[string]int, len(left))
	for _, value := range left {
		want[value]++
	}
	for _, value := range right {
		want[value]--
		if want[value] < 0 {
			return false
		}
	}
	return true
}

func TestRedisDirectoryRunHeartbeatLoopStopsOnContextCancel(t *testing.T) {
	dir, _ := newTestDirectory(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- dir.RunHeartbeatLoop(ctx, "game", "default", time.Hour, 10)
	}()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunHeartbeatLoop error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunHeartbeatLoop did not stop after context cancellation")
	}
}

func TestRedisDirectoryRunHeartbeatLoopPreCanceledNonPositiveInterval(t *testing.T) {
	dir, _ := newTestDirectory(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := dir.RunHeartbeatLoop(ctx, "game", "default", 0, 10); err != nil {
		t.Fatalf("RunHeartbeatLoop error: %v", err)
	}
}

func TestRedisDirectoryExpireHeartbeatsNonPositiveLimitDoesNothing(t *testing.T) {
	dir, _ := newTestDirectory(t)
	count, err := dir.ExpireHeartbeats(context.Background(), "game", "default", time.Now(), 0)
	if err != nil || count != 0 {
		t.Fatalf("ExpireHeartbeats limit 0 = (%d, %v), want (0, nil)", count, err)
	}
}

func TestRedisDirectoryHeartbeatRawStoresOfflineStatus(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	dir.SetHeartbeatTTL(time.Second)
	staleAt := time.Now().Add(-time.Minute)
	node := registerHeartbeatTestNode(t, dir, "game-1", "session-a", staleAt)
	if _, err := dir.ExpireHeartbeats(ctx, node.NodeType, node.NodeGroup, staleAt.Add(2*time.Second), 1); err != nil {
		t.Fatalf("ExpireHeartbeats error: %v", err)
	}
	raw, err := client.Get(ctx, NodeKey(node.NodeIdentity)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var stored sp.Node
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status != sp.NodeStatusOffline {
		t.Fatalf("stored status = %s, want offline", stored.Status)
	}
}

func TestRedisDirectoryRenewNodeRejectsOfflineNodeWithoutWrites(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	dir.SetHeartbeatTTL(time.Second)
	staleAt := time.Now().Add(-time.Minute)
	node := registerHeartbeatTestNode(t, dir, "game-1", "session-a", staleAt)
	if _, err := dir.ExpireHeartbeats(ctx, node.NodeType, node.NodeGroup, staleAt.Add(2*time.Second), 1); err != nil {
		t.Fatalf("ExpireHeartbeats error: %v", err)
	}
	nodeKey := NodeKey(node.NodeIdentity)
	heartbeatKey := NodeHeartbeatKey(node.NodeType, node.NodeGroup)
	rawBefore := client.Get(ctx, nodeKey).Val()
	eventsBefore := client.XLen(ctx, EventsStreamKey()).Val()

	if err := dir.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeNotFound) {
		t.Fatalf("RenewNode offline error = %v, want ErrNodeNotFound", err)
	}
	if raw := client.Get(ctx, nodeKey).Val(); raw != rawBefore {
		t.Fatalf("raw changed after rejected renew: got %q, want %q", raw, rawBefore)
	}
	if _, err := client.ZScore(ctx, heartbeatKey, nodeKey).Result(); !errors.Is(err, goredis.Nil) {
		t.Fatalf("offline heartbeat re-added, error = %v", err)
	}
	if events := client.XLen(ctx, EventsStreamKey()).Val(); events != eventsBefore {
		t.Fatalf("events changed after rejected renew: got %d, want %d", events, eventsBefore)
	}
}

func registerHeartbeatTestNode(t *testing.T, dir *Directory, name, session string, heartbeatAt time.Time) sp.Node {
	t.Helper()
	node := sp.Node{
		NodeType:        "game",
		NodeGroup:       "default",
		NodeName:        name,
		NodeSessionID:   session,
		Status:          sp.NodeStatusActive,
		LastHeartbeatAt: heartbeatAt,
	}
	if err := dir.RegisterNode(context.Background(), node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	node.NodeIdentity = node.NodeType + "/" + node.NodeGroup + "/" + node.NodeName
	return node
}
