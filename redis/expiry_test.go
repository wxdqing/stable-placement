package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type getHookClient struct {
	goredis.UniversalClient
	afterGet func(key string)
}

type blockingZRangeClient struct {
	goredis.UniversalClient
	started chan struct{}
}

func (c blockingZRangeClient) ZRangeByScore(ctx context.Context, key string, opt *goredis.ZRangeBy) *goredis.StringSliceCmd {
	close(c.started)
	<-ctx.Done()
	return goredis.NewStringSliceResult(nil, ctx.Err())
}

func (c getHookClient) Get(ctx context.Context, key string) *goredis.StringCmd {
	result := c.UniversalClient.Get(ctx, key)
	if c.afterGet != nil {
		c.afterGet(key)
	}
	return result
}

func TestRedisDirectoryLookupRejectsExpiredLease(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	registerTestNode(t, dir, "game-1", "session-a")

	placement := allocateExpiringPlacement(t, dir, "expired-lookup")
	waitUntilExpired(placement.LeaseExpireAt)

	if _, err := dir.Lookup(ctx, placement.GrainKey); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Lookup expired lease err = %v, want ErrPlacementNotFound", err)
	}
}

func TestRedisDirectoryAllocateReplacesExpiredPlacement(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	oldNode := registerTestNode(t, dir, "game-1", "session-a")
	newNode := registerTestNode(t, dir, "game-2", "session-b")

	old := allocateExpiringPlacement(t, dir, "expired-allocate")
	waitUntilExpired(old.LeaseExpireAt)

	replacement, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         old.GrainID,
		Kind:            old.Kind,
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate replacement error: %v", err)
	}
	if replacement.Version != old.Version+1 {
		t.Fatalf("replacement version = %d, want %d", replacement.Version, old.Version+1)
	}
	if replacement.Status != sp.PlacementStatusActive {
		t.Fatalf("replacement status = %q, want active", replacement.Status)
	}
	if replacement.NodeIdentity != newNode.NodeIdentity {
		t.Fatalf("replacement node = %q, want %q", replacement.NodeIdentity, newNode.NodeIdentity)
	}
	if score, err := client.ZScore(ctx, PlacementNodeKey(oldNode.NodeIdentity), old.GrainKey.String()).Result(); err == nil {
		t.Fatalf("old node index still contains placement with score %v", score)
	}
	if _, err := client.ZScore(ctx, PlacementNodeKey(newNode.NodeIdentity), old.GrainKey.String()).Result(); err != nil {
		t.Fatalf("new node index missing placement: %v", err)
	}
	events := client.XRange(ctx, EventsStreamKey(), "-", "+").Val()
	created := 0
	for _, event := range events {
		if event.Values["type"] == string(sp.EventPlacementCreated) && event.Values["grain_key"] == old.GrainKey.String() {
			created++
		}
	}
	if created != 2 {
		t.Fatalf("PlacementCreated events = %d, want 2", created)
	}
}

func TestRedisDirectoryAllocateRetriesExpiredPlacementConflict(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	dir, err := NewDirectory(base, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	registerTestNode(t, dir, "game-1", "session-a")
	old := allocateExpiringPlacement(t, dir, "expired-conflict")
	waitUntilExpired(old.LeaseExpireAt)

	concurrent := *old
	concurrent.Version++
	concurrent.UpdateTime = time.Now()
	concurrent.Lease.Version++
	concurrent.LeaseExpireAt = time.Now().Add(time.Minute)
	concurrent.Lease.ExpireAt = concurrent.LeaseExpireAt
	concurrentRaw, err := json.Marshal(concurrent)
	if err != nil {
		t.Fatal(err)
	}
	var changed atomic.Bool
	var reads atomic.Int64
	dir.client = evalHookClient{
		UniversalClient: getHookClient{
			UniversalClient: base,
			afterGet: func(key string) {
				if key == PlacementKey(old.GrainKey) {
					reads.Add(1)
				}
			},
		},
		beforeEval: func(script string) {
			if script != allocateLua || !changed.CompareAndSwap(false, true) {
				return
			}
			if err := base.Set(ctx, PlacementKey(old.GrainKey), concurrentRaw, 0).Err(); err != nil {
				t.Errorf("concurrent Set error: %v", err)
			}
			if err := base.ZAdd(ctx, LeaseExpireKey(), goredis.Z{
				Score:  float64(concurrent.LeaseExpireAt.UnixMilli()),
				Member: concurrent.GrainKey.String(),
			}).Err(); err != nil {
				t.Errorf("concurrent ZAdd error: %v", err)
			}
		},
	}

	stored, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         old.GrainID,
		Kind:            old.Kind,
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate after conflict error: %v", err)
	}
	if stored.Version != concurrent.Version ||
		stored.Lease.Version != concurrent.Lease.Version ||
		!stored.LeaseExpireAt.Equal(concurrent.LeaseExpireAt) {
		t.Fatalf("Allocate overwrote concurrent placement: got %+v, want %+v", stored, concurrent)
	}
	if reads.Load() != 2 {
		t.Fatalf("placement pre-reads = %d, want 2 after conflict retry", reads.Load())
	}
}

func TestRedisDirectoryAllocateKeepsExpiredPlacementIndexWhenNoNodeAvailable(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	node := registerTestNode(t, dir, "game-1", "session-a")
	old := allocateExpiringPlacement(t, dir, "expired-no-candidate")
	waitUntilExpired(old.LeaseExpireAt)
	oldRaw, err := client.Get(ctx, PlacementKey(old.GrainKey)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.MarkNodeInvalid(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatal(err)
	}

	_, err = dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         old.GrainID,
		Kind:            old.Kind,
		TargetNodeType:  node.NodeType,
		TargetNodeGroup: node.NodeGroup,
		LeaseTTL:        time.Minute,
	})
	if !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("Allocate error = %v, want ErrNoAvailableNode", err)
	}
	if raw := client.Get(ctx, PlacementKey(old.GrainKey)).Val(); raw != oldRaw {
		t.Fatalf("placement changed without a replacement: got %q, want %q", raw, oldRaw)
	}
	if _, err := client.ZScore(ctx, PlacementNodeKey(node.NodeIdentity), old.GrainKey.String()).Result(); err != nil {
		t.Fatalf("old node index removed without a replacement: %v", err)
	}
}

func TestRedisDirectoryExpireDueHonorsBatch(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	registerTestNode(t, dir, "game-1", "session-a")

	placements := []*sp.Placement{
		allocateExpiringPlacement(t, dir, "expired-batch-1"),
		allocateExpiringPlacement(t, dir, "expired-batch-2"),
		allocateExpiringPlacement(t, dir, "expired-batch-3"),
	}
	now := placements[2].LeaseExpireAt.Add(time.Millisecond)

	count, err := dir.ExpireDue(ctx, now, 2)
	if err != nil {
		t.Fatalf("ExpireDue error: %v", err)
	}
	if count != 2 {
		t.Fatalf("ExpireDue count = %d, want 2", count)
	}
	expired := 0
	for _, placement := range placements {
		stored, err := dir.getPlacement(ctx, placement.GrainKey)
		if err != nil {
			t.Fatalf("getPlacement(%q) error: %v", placement.GrainKey, err)
		}
		if stored.Status == sp.PlacementStatusExpired {
			expired++
		}
	}
	if expired != 2 {
		t.Fatalf("expired placements = %d, want 2", expired)
	}
	if remaining := client.ZCount(ctx, LeaseExpireKey(), "-inf", "("+formatUnixMilli(now)).Val(); remaining != 1 {
		t.Fatalf("remaining due lease members = %d, want 1", remaining)
	}
}

func TestRedisDirectoryExpireDueToleratesConcurrentBusinessErrors(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	dir, err := NewDirectory(base, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	registerTestNode(t, dir, "game-1", "session-a")
	due := allocateExpiringPlacement(t, dir, "due-after-conflicts")
	now := due.LeaseExpireAt.Add(time.Millisecond)

	missingKey, _ := sp.NewGrainKey("Player", "missing-during-expire")
	if err := base.ZAdd(ctx, LeaseExpireKey(), goredis.Z{Score: float64(now.Add(-time.Second).UnixMilli()), Member: missingKey.String()}).Err(); err != nil {
		t.Fatal(err)
	}
	notExpired, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "renewed-during-expire",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := base.ZAdd(ctx, LeaseExpireKey(), goredis.Z{Score: float64(now.Add(-time.Second).UnixMilli()), Member: notExpired.GrainKey.String()}).Err(); err != nil {
		t.Fatal(err)
	}

	conflict := allocateExpiringPlacement(t, dir, "version-conflict")
	var changed atomic.Bool
	dir.client = getHookClient{
		UniversalClient: base,
		afterGet: func(key string) {
			if key != PlacementKey(conflict.GrainKey) || !changed.CompareAndSwap(false, true) {
				return
			}
			updated := *conflict
			updated.Lease.Version++
			raw, marshalErr := json.Marshal(updated)
			if marshalErr != nil {
				t.Errorf("marshal concurrent placement: %v", marshalErr)
				return
			}
			if setErr := base.Set(ctx, key, raw, 0).Err(); setErr != nil {
				t.Errorf("concurrent Set error: %v", setErr)
			}
		},
	}

	count, err := dir.ExpireDue(ctx, now.Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ExpireDue error: %v", err)
	}
	if count != 1 {
		t.Fatalf("ExpireDue count = %d, want 1", count)
	}
	stored, err := dir.getPlacement(ctx, due.GrainKey)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != sp.PlacementStatusExpired {
		t.Fatalf("due placement status = %q, want expired", stored.Status)
	}
}

func TestRedisDirectoryExpireDueReturnsRedisDataError(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	key, _ := sp.NewGrainKey("Player", "corrupt")
	if err := client.Set(ctx, PlacementKey(key), "not-json", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, LeaseExpireKey(), goredis.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: key.String()}).Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := dir.ExpireDue(ctx, time.Now(), 1); err == nil {
		t.Fatal("ExpireDue error = nil, want Redis data error")
	}
}

func TestRedisDirectoryExpireDueReturnsRedisCommandError(t *testing.T) {
	dir, client := newTestDirectory(t)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := dir.ExpireDue(context.Background(), time.Now(), 1); err == nil {
		t.Fatal("ExpireDue error = nil, want Redis command error")
	}
}

func TestRedisDirectoryRunExpireLoopStopsOnContextCancel(t *testing.T) {
	dir, _ := newTestDirectory(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- dir.RunExpireLoop(ctx, time.Hour, 10)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunExpireLoop error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunExpireLoop did not stop after context cancellation")
	}
}

func TestRedisDirectoryRunExpireLoopTreatsCancellationDuringScanAsCleanExit(t *testing.T) {
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	started := make(chan struct{})
	dir, err := NewDirectory(blockingZRangeClient{UniversalClient: base, started: started}, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- dir.RunExpireLoop(ctx, time.Millisecond, 10)
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("RunExpireLoop did not start a scan")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunExpireLoop cancellation error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunExpireLoop did not stop after cancellation during scan")
	}
}

func TestRedisDirectoryExpireDueNonPositiveLimitDoesNothing(t *testing.T) {
	dir, _ := newTestDirectory(t)
	count, err := dir.ExpireDue(context.Background(), time.Now(), 0)
	if err != nil || count != 0 {
		t.Fatalf("ExpireDue limit 0 = (%d, %v), want (0, nil)", count, err)
	}
}

func allocateExpiringPlacement(t *testing.T, dir *Directory, grainID string) *sp.Placement {
	t.Helper()
	placement, err := dir.Allocate(context.Background(), sp.AllocateCommand{
		GrainID:         grainID,
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Allocate(%q) error: %v", grainID, err)
	}
	return placement
}

func waitUntilExpired(expireAt time.Time) {
	if delay := time.Until(expireAt); delay > 0 {
		time.Sleep(delay)
	}
	for time.Now().Before(expireAt) {
		time.Sleep(time.Millisecond)
	}
}

func formatUnixMilli(value time.Time) string {
	return strconv.FormatInt(value.UnixMilli(), 10)
}
