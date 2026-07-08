package redis

import (
	"errors"
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

func TestNewStreamConsumerRequiresNodeSession(t *testing.T) {
	_, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1"})
	if !errors.Is(err, ErrSharedConsumerGroup) {
		t.Fatalf("err = %v, want ErrSharedConsumerGroup", err)
	}
}

func TestNewStreamConsumerUsesUniqueGroupPerNodeSession(t *testing.T) {
	first, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("NewStreamConsumer first error: %v", err)
	}
	second, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	if err != nil {
		t.Fatalf("NewStreamConsumer second error: %v", err)
	}
	if first.Group == second.Group {
		t.Fatalf("groups are equal: %q", first.Group)
	}
}
