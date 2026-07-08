package strategies

import (
	"context"
	"sync"

	sp "github.com/wxdqing/stable-placement"
)

type RoundRobin struct {
	mu   sync.Mutex
	next int
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

func (r *RoundRobin) Choose(_ context.Context, input sp.StrategyInput) (sp.Node, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(input.EffectiveNodes) == 0 {
		return sp.Node{}, sp.ErrNoAvailableNode
	}
	node := input.EffectiveNodes[r.next%len(input.EffectiveNodes)]
	r.next++
	return node, nil
}
