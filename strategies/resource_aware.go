package strategies

import (
	"context"
	"sort"
	"sync/atomic"

	sp "github.com/wxdqing/stable-placement"
)

type ResourceAwareConfig = sp.ResourceAwareConfig

type ResourceAware struct {
	next   atomic.Uint64
	config ResourceAwareConfig
}

func NewResourceAware(config ResourceAwareConfig) (*ResourceAware, error) {
	var err error
	config, err = sp.NormalizeResourceAwareConfig(config)
	if err != nil {
		return nil, err
	}
	return &ResourceAware{config: config}, nil
}

type resourceCandidate struct {
	node            sp.Node
	memoryBucket    int64
	cpuBucket       int64
	goroutineBucket int64
	placementCount  int
}

func (r *ResourceAware) Choose(_ context.Context, input sp.StrategyInput) (sp.Node, error) {
	nowMillis := r.config.Now().UnixMilli()
	candidates := make([]resourceCandidate, 0, len(input.EffectiveNodes))
	for _, node := range input.EffectiveNodes {
		metrics := node.Metrics
		if !r.usable(metrics, nowMillis) {
			continue
		}
		candidates = append(candidates, resourceCandidate{
			node:            node,
			memoryBucket:    metrics.MemoryAvailableBytes / sp.ResourceMemoryBucketBytes,
			cpuBucket:       metrics.CPUAvailableMilliCores / sp.ResourceCPUBucketMilliCores,
			goroutineBucket: metrics.Goroutines / sp.ResourceGoroutineBucketSize,
			placementCount:  input.PlacementCounts[node.NodeIdentity],
		})
	}
	if len(candidates) == 0 {
		return sp.Node{}, sp.ErrNoAvailableNode
	}
	sort.Slice(candidates, func(i, j int) bool {
		if comparison := compareResourceCandidates(candidates[i], candidates[j]); comparison != 0 {
			return comparison < 0
		}
		return candidates[i].node.NodeIdentity < candidates[j].node.NodeIdentity
	})
	tied := 1
	for tied < len(candidates) && compareResourceCandidates(candidates[0], candidates[tied]) == 0 {
		tied++
	}
	next := r.next.Add(1) - 1
	chosen := candidates[next%uint64(tied)].node
	return chosen, nil
}

func (r *ResourceAware) usable(metrics sp.NodeMetrics, nowMillis int64) bool {
	if sp.ValidateNodeMetrics(metrics) != nil || metrics.UpdatedAtUnixMilli <= 0 || metrics.UpdatedAtUnixMilli > nowMillis || nowMillis-metrics.UpdatedAtUnixMilli > r.config.MetricsMaxAge.Milliseconds() {
		return false
	}
	if metrics.MemoryAvailableBytes < r.config.MinMemoryAvailableBytes || metrics.CPUAvailableMilliCores < r.config.MinCPUAvailableMilliCores {
		return false
	}
	return r.config.MaxGoroutines == 0 || metrics.Goroutines <= r.config.MaxGoroutines
}

func compareResourceCandidates(left, right resourceCandidate) int {
	if left.memoryBucket != right.memoryBucket {
		if left.memoryBucket > right.memoryBucket {
			return -1
		}
		return 1
	}
	if left.cpuBucket != right.cpuBucket {
		if left.cpuBucket > right.cpuBucket {
			return -1
		}
		return 1
	}
	if left.goroutineBucket != right.goroutineBucket {
		if left.goroutineBucket < right.goroutineBucket {
			return -1
		}
		return 1
	}
	if left.placementCount < right.placementCount {
		return -1
	}
	if left.placementCount > right.placementCount {
		return 1
	}
	return 0
}
