package stableplacement

import (
	"context"
	"fmt"
	"time"
)

const (
	DefaultResourceMetricsMaxAge             = 10 * time.Second
	DefaultResourceMinMemoryAvailableBytes   = int64(256 << 20)
	DefaultResourceMinCPUAvailableMilliCores = int64(100)
	ResourceMemoryBucketBytes                = int64(256 << 20)
	ResourceCPUBucketMilliCores              = int64(100)
	ResourceGoroutineBucketSize              = int64(100)
)

type ResourceAwareConfig struct {
	MetricsMaxAge             time.Duration
	MinMemoryAvailableBytes   int64
	MinCPUAvailableMilliCores int64
	MaxGoroutines             int64
	Now                       func() time.Time
}

func NormalizeResourceAwareConfig(config ResourceAwareConfig) (ResourceAwareConfig, error) {
	if config.MetricsMaxAge == 0 {
		config.MetricsMaxAge = DefaultResourceMetricsMaxAge
	}
	if config.MinMemoryAvailableBytes == 0 {
		config.MinMemoryAvailableBytes = DefaultResourceMinMemoryAvailableBytes
	}
	if config.MinCPUAvailableMilliCores == 0 {
		config.MinCPUAvailableMilliCores = DefaultResourceMinCPUAvailableMilliCores
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.MetricsMaxAge < 0 || config.MinMemoryAvailableBytes < 0 || config.MinCPUAvailableMilliCores < 0 || config.MaxGoroutines < 0 {
		return ResourceAwareConfig{}, fmt.Errorf("%w: invalid resource-aware strategy config", ErrPlacementConfigInvalid)
	}
	return config, nil
}

// StrategyMode 声明 Allocate 时策略的执行方式。
//
// memory.Directory 必须使用 StrategyModeGo，并在进程内调用 PlacementStrategy。
// redis.Directory 必须使用 StrategyModeRedisRoundRobin，在 Redis Lua 中原子执行 RoundRobin；
// 传入 PlacementStrategy 不会生效。
type StrategyMode string

const (
	// StrategyModeGo 在进程内执行 PlacementStrategy，仅供 memory.Directory 使用。
	StrategyModeGo StrategyMode = "go"
	// StrategyModeRedisRoundRobin 在 Redis Lua 中原子执行 RoundRobin，仅供 redis.Directory 使用。
	StrategyModeRedisRoundRobin StrategyMode = "redis_round_robin"
	// StrategyModeRedisResourceAware selects nodes from resource snapshots in Redis Lua.
	StrategyModeRedisResourceAware StrategyMode = "redis_resource_aware"
)

type StrategyInput struct {
	// GrainID 是待分配的业务实体 ID。
	GrainID string
	// Kind 是待分配的业务实体类型。
	Kind string
	// NodeType 是候选节点类型。
	NodeType string
	// NodeGroup 是候选节点分组。
	NodeGroup string
	// EffectiveNodes 是已经过滤失效节点后的可选节点集合。
	EffectiveNodes []Node
	// PlacementCounts contains the active placement count for each node identity.
	PlacementCounts map[string]int
}

type PlacementStrategy interface {
	Choose(ctx context.Context, input StrategyInput) (Node, error)
}
