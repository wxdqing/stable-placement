package redis

import (
	"strings"
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

func TestNewStreamConsumerUsesV2Group(t *testing.T) {
	node := sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"}
	consumer, err := NewStreamConsumer(node)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(consumer.Group, ":v2:") {
		t.Fatalf("group = %q", consumer.Group)
	}
}

func TestNewStreamConsumerRejectsSharedIdentity(t *testing.T) {
	for _, node := range []sp.Node{{NodeSessionID: "session-a"}, {NodeIdentity: "game/default/game-1"}} {
		if _, err := NewStreamConsumer(node); err != ErrSharedConsumerGroup {
			t.Fatalf("err = %v", err)
		}
	}
}
