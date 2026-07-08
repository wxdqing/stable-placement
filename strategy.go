package stableplacement

import "context"

type StrategyInput struct {
	GrainID        string
	Kind           string
	NodeType       string
	NodeGroup      string
	EffectiveNodes []Node
}

type PlacementStrategy interface {
	Choose(ctx context.Context, input StrategyInput) (Node, error)
}
