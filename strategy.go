package stableplacement

import "context"

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
