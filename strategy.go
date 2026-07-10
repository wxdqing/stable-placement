package stableplacement

import "context"

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
}

type PlacementStrategy interface {
	Choose(ctx context.Context, input StrategyInput) (Node, error)
}
