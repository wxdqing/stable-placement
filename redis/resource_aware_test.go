package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func newResourceDirectory(t *testing.T) (*Directory, *goredis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(time.Unix(100, 0))
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	directory, err := NewDirectory(client, sp.StrategyModeRedisResourceAware, sp.NodeLeaseConfig{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return directory, client, server
}

func renewResourceNode(t *testing.T, directory *Directory, node sp.Node, metrics sp.NodeMetrics) {
	t.Helper()
	ctx := context.Background()
	if _, err := directory.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID, Metrics: &metrics}); err != nil {
		t.Fatal(err)
	}
}

func TestRedisResourceAwareOrdering(t *testing.T) {
	tests := []struct {
		name          string
		firstMetrics  sp.NodeMetrics
		secondMetrics sp.NodeMetrics
		seedFirst     bool
		want          string
	}{
		{name: "memory bucket", firstMetrics: sp.NodeMetrics{MemoryAvailableBytes: 256 << 20, CPUAvailableMilliCores: 900}, secondMetrics: sp.NodeMetrics{MemoryAvailableBytes: 512 << 20, CPUAvailableMilliCores: 100}, want: "game/default/game-2"},
		{name: "cpu bucket", firstMetrics: sp.NodeMetrics{MemoryAvailableBytes: 512 << 20, CPUAvailableMilliCores: 100}, secondMetrics: sp.NodeMetrics{MemoryAvailableBytes: 512 << 20, CPUAvailableMilliCores: 200}, want: "game/default/game-2"},
		{name: "goroutine bucket", firstMetrics: sp.NodeMetrics{MemoryAvailableBytes: 512 << 20, CPUAvailableMilliCores: 200, Goroutines: 199}, secondMetrics: sp.NodeMetrics{MemoryAvailableBytes: 512 << 20, CPUAvailableMilliCores: 200, Goroutines: 99}, want: "game/default/game-2"},
		{name: "placement count", firstMetrics: sp.NodeMetrics{MemoryAvailableBytes: 512 << 20, CPUAvailableMilliCores: 200}, secondMetrics: sp.NodeMetrics{MemoryAvailableBytes: 512 << 20, CPUAvailableMilliCores: 200}, seedFirst: true, want: "game/default/game-2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory, client, _ := newResourceDirectory(t)
			first := testNode("game-1", "session-a")
			second := testNode("game-2", "session-b")
			renewResourceNode(t, directory, first, test.firstMetrics)
			renewResourceNode(t, directory, second, test.secondMetrics)
			if test.seedFirst {
				if err := client.ZAdd(context.Background(), PlacementNodeKey(first.NodeIdentity), goredis.Z{Score: 1, Member: "seed"}).Err(); err != nil {
					t.Fatal(err)
				}
			}
			placement, err := directory.Allocate(context.Background(), sp.AllocateCommand{GrainID: "1", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			if err != nil || placement.NodeIdentity != test.want {
				t.Fatalf("Allocate = %+v, %v; want %q", placement, err, test.want)
			}
		})
	}
}

func TestRedisResourceAwareFiltersStaleAndUnsafeMetrics(t *testing.T) {
	directory, _, server := newResourceDirectory(t)
	stale := testNode("game-1", "session-a")
	renewResourceNode(t, directory, stale, sp.NodeMetrics{MemoryAvailableBytes: 1 << 30, CPUAvailableMilliCores: 1000})
	server.SetTime(time.Unix(111, 0))
	_, err := directory.Allocate(context.Background(), sp.AllocateCommand{GrainID: "1", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("stale metrics error = %v", err)
	}
}

func TestRedisResourceAwareUsesConfiguredMetricsMaxAge(t *testing.T) {
	server := miniredis.RunT(t)
	server.SetTime(time.Unix(100, 0))
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	directory, err := NewDirectory(client, sp.StrategyModeRedisResourceAware, sp.NodeLeaseConfig{TTL: time.Minute}, sp.ResourceAwareConfig{MetricsMaxAge: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	renewResourceNode(t, directory, testNode("game-1", "session-a"), sp.NodeMetrics{MemoryAvailableBytes: 1 << 30, CPUAvailableMilliCores: 1000})
	server.SetTime(time.Unix(103, 0))
	_, err = directory.Allocate(context.Background(), sp.AllocateCommand{GrainID: "1", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("configured stale metrics error = %v", err)
	}
}

func TestRedisResourceAwareRoundRobinTieAndAtomicCounts(t *testing.T) {
	directory, _, _ := newResourceDirectory(t)
	metrics := sp.NodeMetrics{MemoryAvailableBytes: 512 << 20, CPUAvailableMilliCores: 200, Goroutines: 10}
	for _, node := range []sp.Node{testNode("game-1", "session-a"), testNode("game-2", "session-b")} {
		renewResourceNode(t, directory, node, metrics)
	}
	const allocations = 20
	counts := map[string]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for index := 0; index < allocations; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			placement, err := directory.Allocate(context.Background(), sp.AllocateCommand{GrainID: fmt.Sprintf("%d", index), Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
			if err != nil {
				t.Errorf("Allocate %d: %v", index, err)
				return
			}
			mu.Lock()
			counts[placement.NodeIdentity]++
			mu.Unlock()
		}(index)
	}
	wg.Wait()
	if counts["game/default/game-1"] != allocations/2 || counts["game/default/game-2"] != allocations/2 {
		t.Fatalf("allocation counts = %+v", counts)
	}
}

func TestRedisResourceAwareResolveKeepsUsableOwner(t *testing.T) {
	directory, _, _ := newResourceDirectory(t)
	owner := testNode("game-1", "session-a")
	renewResourceNode(t, directory, owner, sp.NodeMetrics{MemoryAvailableBytes: 256 << 20, CPUAvailableMilliCores: 100})
	first, err := directory.ResolveRoute(context.Background(), sp.ResolveRouteCommand{GrainID: "1", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil {
		t.Fatal(err)
	}
	better := testNode("game-2", "session-b")
	renewResourceNode(t, directory, better, sp.NodeMetrics{MemoryAvailableBytes: 1 << 30, CPUAvailableMilliCores: 1000})
	resolved, err := directory.ResolveRoute(context.Background(), sp.ResolveRouteCommand{GrainID: "1", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default"})
	if err != nil || resolved.NodeIdentity != first.NodeIdentity {
		t.Fatalf("ResolveRoute = %+v, %v; first = %+v", resolved, err, first)
	}
}
