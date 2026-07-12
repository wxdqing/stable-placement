package redis

import (
	"context"
	"strings"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func TestKeysUseV2StablePlacementNamespace(t *testing.T) {
	grain, _ := sp.NewGrainKey("Player", "10001")
	keys := []string{
		PlacementKey(grain), PlacementNodeKey("game/default/game-1"),
		NodesKey("game", "default"), NodeKey("game/default/game-1"),
		InvalidNodesKey("game", "default"), NodeLeaseKey("game", "default"),
		EventsStreamKey(), EventsPubSubChannelKey(), AuditStreamKey(), SequenceKey(),
		StrategyRoundRobinKey("game", "default"),
	}
	for _, key := range keys {
		if !HasStablePlacementHashTag(key) || !strings.Contains(key, ":v2:") {
			t.Fatalf("key %q must use the v2 stable-placement namespace", key)
		}
	}
	if NamespaceVersion != "v2" || NamespacePrefix != "sp:{stable-placement}:v2:" {
		t.Fatalf("namespace = %q/%q", NamespaceVersion, NamespacePrefix)
	}
}

func TestKeysEncodeUserValues(t *testing.T) {
	for _, key := range []string{
		PlacementNodeKey("game/default/game-1"),
		NodesKey("game/type", "default/group"),
		NodeLeaseKey("game/type", "default/group"),
	} {
		if strings.Contains(key, "game/") || strings.Contains(key, "default/") {
			t.Fatalf("key contains raw user value: %s", key)
		}
	}
}

func TestConsumerGroupUsesV2AndSessionIdentity(t *testing.T) {
	group := ConsumerGroupName("game/default/game-1", "session-a")
	if !strings.Contains(group, ":v2:") {
		t.Fatalf("group %q does not use v2", group)
	}
	if group == ConsumerGroupName("game/default/game-1", "session-b") ||
		group == ConsumerGroupName("game/default/game-2", "session-a") {
		t.Fatal("consumer group does not include identity and session")
	}
}

func TestV2NamespaceDoesNotReadOrModifyV1(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	v1 := map[string]string{
		"sp:{stable-placement}:node:old":      `{"NodeIdentity":"old"}`,
		"sp:{stable-placement}:placement:old": `{"GrainKey":"old"}`,
		"sp:{stable-placement}:events:stream": "old-stream-sentinel",
		"sp:{stable-placement}:seq":           "41",
	}
	for key, value := range v1 {
		if err := client.Set(ctx, key, value, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	grain := sp.GrainKey("Player/old")
	for _, key := range []string{NodeKey("old"), PlacementKey(grain), EventsStreamKey(), SequenceKey()} {
		if client.Exists(ctx, key).Val() != 0 {
			t.Fatalf("v2 key %q unexpectedly reads v1", key)
		}
		if err := client.Set(ctx, key, "v2", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for key, want := range v1 {
		if got := client.Get(ctx, key).Val(); got != want {
			t.Fatalf("v1 %q = %q, want %q", key, got, want)
		}
	}
}
