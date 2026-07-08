package strategies

import (
	"context"
	"errors"
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

func TestRoundRobinChoosesNodesInOrder(t *testing.T) {
	strategy := NewRoundRobin()
	nodes := []sp.Node{
		{NodeIdentity: "game/default/game-1"},
		{NodeIdentity: "game/default/game-2"},
	}

	first, err := strategy.Choose(context.Background(), sp.StrategyInput{EffectiveNodes: nodes})
	if err != nil {
		t.Fatalf("first choose error: %v", err)
	}
	second, err := strategy.Choose(context.Background(), sp.StrategyInput{EffectiveNodes: nodes})
	if err != nil {
		t.Fatalf("second choose error: %v", err)
	}

	if first.NodeIdentity != "game/default/game-1" || second.NodeIdentity != "game/default/game-2" {
		t.Fatalf("round robin chose %q then %q", first.NodeIdentity, second.NodeIdentity)
	}
}

func TestRoundRobinRejectsEmptyNodes(t *testing.T) {
	_, err := NewRoundRobin().Choose(context.Background(), sp.StrategyInput{})
	if !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("err = %v, want ErrNoAvailableNode", err)
	}
}
