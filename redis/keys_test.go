package redis

import (
	"strings"
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

func TestKeysUseStablePlacementHashTag(t *testing.T) {
	key, _ := sp.NewGrainKey("Player", "10001")
	keys := []string{
		PlacementKey(key),
		PlacementNodeKey("game/default/game-1"),
		NodesKey("game", "default"),
		NodeKey("game/default/game-1"),
		InvalidNodesKey("game", "default"),
		EventsStreamKey(),
		EventsPubSubChannelKey(),
		AuditStreamKey(),
		LeaseExpireKey(),
		SequenceKey(),
		StrategyRoundRobinKey("game", "default"),
	}
	for _, redisKey := range keys {
		if !HasStablePlacementHashTag(redisKey) {
			t.Fatalf("key %q does not contain stable-placement hash tag", redisKey)
		}
	}
}

func TestKeysEncodeSlashBearingValues(t *testing.T) {
	key := PlacementNodeKey("game/default/game-1")
	if strings.Contains(key, "game/default/game-1") {
		t.Fatalf("key contains raw node identity: %s", key)
	}
}

func TestConsumerGroupNameContainsNodeIdentityAndSession(t *testing.T) {
	group := ConsumerGroupName("game/default/game-1", "session-a")
	if group == ConsumerGroupName("game/default/game-1", "session-b") {
		t.Fatal("consumer group does not include session")
	}
	if group == ConsumerGroupName("game/default/game-2", "session-a") {
		t.Fatal("consumer group does not include node identity")
	}
}
