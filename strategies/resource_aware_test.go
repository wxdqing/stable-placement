package strategies

import (
	"context"
	"errors"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

func TestResourceAwareOrdering(t *testing.T) {
	now := time.Unix(100, 0)
	newStrategy := func(t *testing.T) *ResourceAware {
		t.Helper()
		strategy, err := NewResourceAware(ResourceAwareConfig{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		return strategy
	}
	node := func(identity string, memory, cpu, goroutines int64) sp.Node {
		return sp.Node{NodeIdentity: identity, NodeSessionID: "session-" + identity, Metrics: sp.NodeMetrics{
			MemoryAvailableBytes: memory, CPUAvailableMilliCores: cpu, Goroutines: goroutines, UpdatedAtUnixMilli: now.UnixMilli(),
		}}
	}
	tests := []struct {
		name   string
		nodes  []sp.Node
		counts map[string]int
		want   string
	}{
		{name: "memory bucket", nodes: []sp.Node{node("a", 256<<20, 900, 1), node("b", 512<<20, 100, 1000)}, want: "b"},
		{name: "cpu bucket", nodes: []sp.Node{node("a", 512<<20, 100, 1), node("b", 512<<20, 200, 1000)}, want: "b"},
		{name: "goroutine bucket", nodes: []sp.Node{node("a", 512<<20, 200, 199), node("b", 512<<20, 200, 99)}, want: "b"},
		{name: "placement count", nodes: []sp.Node{node("a", 512<<20, 200, 99), node("b", 512<<20, 200, 99)}, counts: map[string]int{"a": 2, "b": 1}, want: "b"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chosen, err := newStrategy(t).Choose(context.Background(), sp.StrategyInput{EffectiveNodes: test.nodes, PlacementCounts: test.counts})
			if err != nil || chosen.NodeIdentity != test.want {
				t.Fatalf("Choose = %+v, %v; want %q", chosen, err, test.want)
			}
		})
	}
}

func TestResourceAwareFiltersInvalidMetrics(t *testing.T) {
	now := time.Unix(100, 0)
	strategy, err := NewResourceAware(ResourceAwareConfig{MetricsMaxAge: time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	valid := sp.Node{NodeIdentity: "valid", Metrics: sp.NodeMetrics{MemoryAvailableBytes: 256 << 20, CPUAvailableMilliCores: 100, UpdatedAtUnixMilli: now.UnixMilli()}}
	invalid := []sp.Node{
		{NodeIdentity: "missing"},
		{NodeIdentity: "stale", Metrics: sp.NodeMetrics{MemoryAvailableBytes: 1 << 30, CPUAvailableMilliCores: 1000, UpdatedAtUnixMilli: now.Add(-2 * time.Second).UnixMilli()}},
		{NodeIdentity: "future", Metrics: sp.NodeMetrics{MemoryAvailableBytes: 1 << 30, CPUAvailableMilliCores: 1000, UpdatedAtUnixMilli: now.Add(time.Second).UnixMilli()}},
		{NodeIdentity: "negative", Metrics: sp.NodeMetrics{MemoryAvailableBytes: 1 << 30, CPUAvailableMilliCores: -1, UpdatedAtUnixMilli: now.UnixMilli()}},
		{NodeIdentity: "low-memory", Metrics: sp.NodeMetrics{MemoryAvailableBytes: (256 << 20) - 1, CPUAvailableMilliCores: 1000, UpdatedAtUnixMilli: now.UnixMilli()}},
		{NodeIdentity: "low-cpu", Metrics: sp.NodeMetrics{MemoryAvailableBytes: 1 << 30, CPUAvailableMilliCores: 99, UpdatedAtUnixMilli: now.UnixMilli()}},
	}
	chosen, err := strategy.Choose(context.Background(), sp.StrategyInput{EffectiveNodes: append(invalid, valid)})
	if err != nil || chosen.NodeIdentity != valid.NodeIdentity {
		t.Fatalf("Choose = %+v, %v", chosen, err)
	}
	if _, err := strategy.Choose(context.Background(), sp.StrategyInput{EffectiveNodes: invalid}); !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("all invalid metrics error = %v", err)
	}
}

func TestResourceAwareGoroutineLimitAndRoundRobinTieBreak(t *testing.T) {
	now := time.Unix(100, 0)
	strategy, err := NewResourceAware(ResourceAwareConfig{MaxGoroutines: 100, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	metrics := sp.NodeMetrics{MemoryAvailableBytes: 512 << 20, CPUAvailableMilliCores: 200, Goroutines: 100, UpdatedAtUnixMilli: now.UnixMilli()}
	nodes := []sp.Node{{NodeIdentity: "b", Metrics: metrics}, {NodeIdentity: "excluded", Metrics: sp.NodeMetrics{MemoryAvailableBytes: 1 << 30, CPUAvailableMilliCores: 1000, Goroutines: 101, UpdatedAtUnixMilli: now.UnixMilli()}}, {NodeIdentity: "a", Metrics: metrics}}
	for index, want := range []string{"a", "b", "a"} {
		chosen, chooseErr := strategy.Choose(context.Background(), sp.StrategyInput{EffectiveNodes: nodes})
		if chooseErr != nil || chosen.NodeIdentity != want {
			t.Fatalf("Choose %d = %+v, %v; want %q", index, chosen, chooseErr, want)
		}
	}
}

func TestNewResourceAwareRejectsInvalidConfig(t *testing.T) {
	if _, err := NewResourceAware(ResourceAwareConfig{MetricsMaxAge: -time.Second}); !errors.Is(err, sp.ErrPlacementConfigInvalid) {
		t.Fatalf("NewResourceAware error = %v", err)
	}
}
