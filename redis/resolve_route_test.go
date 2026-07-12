package redis

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func resolveRedisPlayer(dir *Directory, grainID, group string) (*sp.PlacementRoute, error) {
	return dir.ResolveRoute(context.Background(), sp.ResolveRouteCommand{
		GrainID: grainID, Kind: "player", TargetNodeType: "game", TargetNodeGroup: group,
	})
}

func TestRedisResolveRouteAllocatesAndReusesOwner(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Minute})
	server.SetTime(time.Unix(1_000, 0))
	node := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	first, err := resolveRedisPlayer(dir, "acct-1", "default")
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolveRedisPlayer(dir, "acct-1", "default")
	if err != nil || first.NodeIdentity != node.NodeIdentity || !sameRouteAuthorization(first, second) || first.ValidUntil.IsZero() || second.ValidUntil.IsZero() {
		t.Fatalf("first=%+v second=%+v err=%v", first, second, err)
	}
	if got := countRedisEvents(client, sp.EventPlacementCreated); got != 1 {
		t.Fatalf("PlacementCreated events = %d", got)
	}
}

func TestRedisResolveRouteRecoversSameIdentityNewSession(t *testing.T) {
	ctx := context.Background()
	dir, _, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Minute})
	server.SetTime(time.Unix(1_000, 0))
	node := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	first, err := resolveRedisPlayer(dir, "acct-1", "default")
	if err != nil {
		t.Fatal(err)
	}
	replacement := node
	replacement.NodeSessionID = "session-b"
	if _, _, err := dir.ReplaceNodeSession(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	route, err := resolveRedisPlayer(dir, "acct-1", "default")
	if err != nil || route.NodeIdentity != node.NodeIdentity || route.OwnerNodeSessionID != "session-b" || route.Version != first.Version+1 {
		t.Fatalf("route=%+v first=%+v err=%v", route, first, err)
	}
}

func TestRedisResolveRouteRequiresManualInvalidForCrossNodeRecovery(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T) (*Directory, sp.Node, sp.Node, *sp.PlacementRoute) {
		t.Helper()
		dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Minute})
		server.SetTime(time.Unix(1_000, 0))
		owner := testNode("game-1", "session-a")
		target := testNode("game-2", "session-b")
		if _, err := dir.RegisterNode(ctx, owner); err != nil {
			t.Fatal(err)
		}
		if _, err := dir.RegisterNode(ctx, target); err != nil {
			t.Fatal(err)
		}
		first, err := resolveRedisPlayer(dir, "acct-1", "default")
		if err != nil {
			t.Fatal(err)
		}
		setRedisNodeStatus(t, client, owner.NodeIdentity, sp.NodeStatusOffline)
		return dir, owner, target, first
	}

	t.Run("not invalid", func(t *testing.T) {
		dir, _, _, first := setup(t)
		route, err := resolveRedisPlayer(dir, "acct-1", "default")
		if !errors.Is(err, sp.ErrPlacementOwnerUnavailable) || route != nil {
			t.Fatalf("route=%+v err=%v", route, err)
		}
		stored, err := dir.getPlacement(ctx, first.GrainKey)
		if err != nil || stored.NodeIdentity != first.NodeIdentity || stored.Version != first.Version {
			t.Fatalf("stored=%+v err=%v", stored, err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		dir, owner, target, first := setup(t)
		if err := dir.MarkNodeInvalid(ctx, owner.NodeType, owner.NodeGroup, owner.NodeName); err != nil {
			t.Fatal(err)
		}
		route, err := resolveRedisPlayer(dir, "acct-1", "default")
		if err != nil || route.NodeIdentity != target.NodeIdentity || route.Version != first.Version+1 {
			t.Fatalf("route=%+v err=%v", route, err)
		}
	})
}

func TestRedisResolveRouteKeepsHealthyInvalidOwnerAndRejectsTargetMismatch(t *testing.T) {
	ctx := context.Background()
	dir, _, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Minute})
	server.SetTime(time.Unix(1_000, 0))
	owner := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, owner); err != nil {
		t.Fatal(err)
	}
	first, err := resolveRedisPlayer(dir, "acct-1", "default")
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.MarkNodeInvalid(ctx, owner.NodeType, owner.NodeGroup, owner.NodeName); err != nil {
		t.Fatal(err)
	}
	route, err := resolveRedisPlayer(dir, "acct-1", "default")
	if err != nil || !sameRouteAuthorization(route, first) {
		t.Fatalf("route=%+v first=%+v err=%v", route, first, err)
	}
	if route, err := resolveRedisPlayer(dir, "acct-1", "other"); !errors.Is(err, sp.ErrPlacementTargetMismatch) || route != nil {
		t.Fatalf("mismatch route=%+v err=%v", route, err)
	}
}

func TestRedisResolveRouteConcurrentCallsCreateOneOwner(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Minute})
	server.SetTime(time.Unix(1_000, 0))
	for _, node := range []sp.Node{testNode("game-1", "session-a"), testNode("game-2", "session-b")} {
		if _, err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	const callers = 50
	routes := make(chan *sp.PlacementRoute, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			route, err := resolveRedisPlayer(dir, "acct-concurrent", "default")
			routes <- route
			errs <- err
		}()
	}
	wg.Wait()
	close(routes)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *sp.PlacementRoute
	for route := range routes {
		if first == nil {
			first = route
			continue
		}
		if !sameRouteAuthorization(route, first) {
			t.Fatalf("route=%+v first=%+v", route, first)
		}
	}
	if got := countRedisEvents(client, sp.EventPlacementCreated); got != 1 {
		t.Fatalf("events=%d", got)
	}
}

func TestRedisResolveRouteOutboxFailureIsAtomic(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Minute})
	server.SetTime(time.Unix(1_000, 0))
	node := testNode("game-1", "session-a")
	if _, err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	client.Del(ctx, EventsStreamKey())
	client.Set(ctx, EventsStreamKey(), "wrongtype", 0)
	if _, err := resolveRedisPlayer(dir, "acct-atomic", "default"); err == nil {
		t.Fatal("expected WRONGTYPE")
	}
	key, _ := sp.NewGrainKey("player", "acct-atomic")
	if client.Exists(ctx, PlacementKey(key)).Val() != 0 || client.Exists(ctx, SequenceKey()).Val() != 0 || client.Exists(ctx, StrategyRoundRobinKey("game", "default")).Val() != 0 {
		t.Fatal("failed ResolveRoute mutated state")
	}
}

func setRedisNodeStatus(t *testing.T, client interface {
	Get(context.Context, string) *goredis.StringCmd
	Set(context.Context, string, interface{}, time.Duration) *goredis.StatusCmd
}, identity string, status sp.NodeStatus) {
	t.Helper()
	ctx := context.Background()
	raw, err := client.Get(ctx, NodeKey(identity)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var node redisNode
	if err := json.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	node.Status = status
	encoded, err := json.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, NodeKey(identity), encoded, 0).Err(); err != nil {
		t.Fatal(err)
	}
}

func countRedisEvents(client *goredis.Client, eventType sp.EventType) int {
	count := 0
	for _, event := range client.XRange(context.Background(), EventsStreamKey(), "-", "+").Val() {
		if event.Values["type"] == string(eventType) {
			count++
		}
	}
	return count
}

func sameRouteAuthorization(left, right *sp.PlacementRoute) bool {
	return left != nil && right != nil && left.GrainKey == right.GrainKey &&
		left.NodeIdentity == right.NodeIdentity && left.OwnerNodeSessionID == right.OwnerNodeSessionID &&
		left.Version == right.Version && left.Status == right.Status && left.NodeLeaseVersion == right.NodeLeaseVersion
}
