package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func addIndexedPlacementV2(t *testing.T, client *goredis.Client, nodeIdentity, grainID string, score float64, status sp.PlacementStatus) sp.GrainKey {
	t.Helper()
	ctx := context.Background()
	key, err := sp.NewGrainKey("Player", grainID)
	if err != nil {
		t.Fatal(err)
	}
	placement := sp.Placement{GrainID: grainID, Kind: "Player", GrainKey: key, NodeIdentity: nodeIdentity, OwnerNodeSessionID: "session-a", Version: 1, Status: status}
	raw, err := json.Marshal(placement)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, PlacementKey(key), raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, PlacementNodeKey(nodeIdentity), goredis.Z{Score: score, Member: key.String()}).Err(); err != nil {
		t.Fatal(err)
	}
	return key
}

func newTestDirectory(t *testing.T, config sp.NodeLeaseConfig) (*Directory, *goredis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	directory, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin, config)
	if err != nil {
		t.Fatal(err)
	}
	return directory, client, server
}

func testNode(name, session string) sp.Node {
	return sp.Node{NodeType: "game", NodeGroup: "default", NodeName: name,
		NodeIdentity: "game/default/" + name, NodeSessionID: session, Address: name + ":8080", Weight: 1}
}

func TestRedisDirectoryImplementsContracts(t *testing.T) {
	var _ sp.Directory = (*Directory)(nil)
	var _ sp.NodeRegistry = (*Directory)(nil)
}

func TestRedisNodeLeaseConfigRejectsNonPositiveTTLAndDefaultsToMinute(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	for _, tc := range []struct {
		name    string
		ttl     time.Duration
		wantErr error
	}{
		{"positive", time.Second, nil}, {"submillisecond", time.Nanosecond, nil},
		{"zero", 0, sp.ErrInvalidNodeLeaseTTL}, {"negative", -time.Second, sp.ErrInvalidNodeLeaseTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: tc.ttl})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
	if sp.DefaultNodeLeaseConfig().TTL != time.Minute {
		t.Fatal("default TTL must be one minute")
	}
	maxDir, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Duration(1<<63 - 1)})
	if err != nil || maxDir.config.TTL <= 0 || maxDir.config.TTL > time.Duration(1<<63-1) {
		t.Fatalf("maximum TTL normalized to %v, err %v", maxDir.config.TTL, err)
	}
}

func readNode(t *testing.T, ctx context.Context, directory *Directory, identity string) sp.Node {
	t.Helper()
	nodes, err := directory.FindNodes(ctx, "game", "default")
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range nodes {
		if node.NodeIdentity == identity {
			return node
		}
	}
	t.Fatalf("node %q not found", identity)
	return sp.Node{}
}

func TestRedisDirectoryFindByNodeUsesBoundedRefill(t *testing.T) {
	ctx := context.Background()
	dir, client, _ := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	identity := "game/default/game-1"
	for i := 0; i < 100; i++ {
		addIndexedPlacementV2(t, client, identity, fmt.Sprintf("released-%03d", i), float64(i+1), sp.PlacementStatusReleased)
	}
	active := addIndexedPlacementV2(t, client, identity, "active", 101, sp.PlacementStatusActive)
	page, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: identity, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Placements) != 1 || page.Placements[0].GrainKey != active || page.NextCursor != "" {
		t.Fatalf("page = %+v", page)
	}
}

func TestRedisDirectoryFindByNodeUsesStableScoreCursor(t *testing.T) {
	ctx := context.Background()
	dir, client, _ := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	identity := "game/default/game-1"
	addIndexedPlacementV2(t, client, identity, "one", 1, sp.PlacementStatusActive)
	second := addIndexedPlacementV2(t, client, identity, "two", 2, sp.PlacementStatusActive)
	third := addIndexedPlacementV2(t, client, identity, "three", 3, sp.PlacementStatusActive)
	first, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: identity, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if first.NextCursor != formatCursor(2, second.String()) {
		t.Fatalf("cursor = %q", first.NextCursor)
	}
	next, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: identity, Cursor: first.NextCursor, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Placements) != 1 || next.Placements[0].GrainKey != third || next.NextCursor != "" {
		t.Fatalf("next = %+v", next)
	}
}

func TestRedisDirectoryFindByNodeRejectsDuplicateScoreAndLaterMalformedData(t *testing.T) {
	ctx := context.Background()
	t.Run("duplicate score", func(t *testing.T) {
		dir, client, _ := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
		identity := "game/default/game-1"
		addIndexedPlacementV2(t, client, identity, "one", 1, sp.PlacementStatusActive)
		addIndexedPlacementV2(t, client, identity, "two", 1, sp.PlacementStatusActive)
		page, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: identity, Limit: 10})
		if err == nil || !strings.Contains(err.Error(), "invalid placement index score") || len(page.Placements) != 0 {
			t.Fatalf("page=%+v err=%v", page, err)
		}
	})
	t.Run("malformed after limit", func(t *testing.T) {
		dir, client, _ := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
		identity := "game/default/game-1"
		addIndexedPlacementV2(t, client, identity, "one", 1, sp.PlacementStatusActive)
		addIndexedPlacementV2(t, client, identity, "two", 2, sp.PlacementStatusActive)
		broken := sp.GrainKey("Player/broken")
		client.Set(ctx, PlacementKey(broken), "{", 0)
		client.ZAdd(ctx, PlacementNodeKey(identity), goredis.Z{Score: 3, Member: broken.String()})
		page, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: identity, Limit: 2})
		if err == nil || len(page.Placements) != 0 || page.NextCursor != "" {
			t.Fatalf("page=%+v err=%v", page, err)
		}
	})
}

func TestRedisDirectoryFindByNodeCursorSurvivesConcurrentIndexChangeV2(t *testing.T) {
	ctx := context.Background()
	dir, client, _ := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	identity := "game/default/game-1"
	firstKey := addIndexedPlacementV2(t, client, identity, "first", 1, sp.PlacementStatusActive)
	secondKey := addIndexedPlacementV2(t, client, identity, "second", 2, sp.PlacementStatusActive)
	thirdKey := addIndexedPlacementV2(t, client, identity, "third", 3, sp.PlacementStatusActive)
	first, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: identity, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Placements) != 1 || first.Placements[0].GrainKey != firstKey || first.NextCursor == "" {
		t.Fatalf("first=%+v", first)
	}
	client.ZRem(ctx, PlacementNodeKey(identity), firstKey.String())
	client.ZAdd(ctx, PlacementNodeKey(identity), goredis.Z{Score: 4, Member: "Player/concurrent"})
	next, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: identity, Cursor: first.NextCursor, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Placements) != 2 || next.Placements[0].GrainKey != secondKey || next.Placements[1].GrainKey != thirdKey {
		t.Fatalf("next=%+v", next)
	}
}
