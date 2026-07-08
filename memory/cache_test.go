package memory

import (
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

func TestCacheDeletesByNodeAndDegradedBypassesReadsAndWrites(t *testing.T) {
	cache := NewPlacementCache()
	key1, _ := sp.NewGrainKey("Player", "10001")
	key2, _ := sp.NewGrainKey("Player", "10002")

	cache.SetCachedPlacement(key1, sp.PlacementRoute{GrainKey: key1, NodeIdentity: "game/default/game-1"})
	cache.SetCachedPlacement(key2, sp.PlacementRoute{GrainKey: key2, NodeIdentity: "game/default/game-1"})
	cache.DeleteCachedPlacementsByNode("game/default/game-1")
	if _, ok := cache.GetCachedPlacement(key1); ok {
		t.Fatal("key1 cache survived DeleteCachedPlacementsByNode")
	}
	if _, ok := cache.GetCachedPlacement(key2); ok {
		t.Fatal("key2 cache survived DeleteCachedPlacementsByNode")
	}

	cache.SetDegraded(true)
	cache.SetCachedPlacement(key1, sp.PlacementRoute{GrainKey: key1, NodeIdentity: "game/default/game-2"})
	if _, ok := cache.GetCachedPlacement(key1); ok {
		t.Fatal("degraded cache returned a value")
	}
	cache.SetDegraded(false)
	if _, ok := cache.GetCachedPlacement(key1); ok {
		t.Fatal("degraded cache wrote a value")
	}
}

func TestCacheDeletesByGroup(t *testing.T) {
	cache := NewPlacementCache()
	key1, _ := sp.NewGrainKey("Player", "10001")
	key2, _ := sp.NewGrainKey("Player", "10002")
	key3, _ := sp.NewGrainKey("Player", "10003")

	cache.SetCachedPlacement(key1, sp.PlacementRoute{GrainKey: key1, NodeIdentity: "game/default/game-1"})
	cache.SetCachedPlacement(key2, sp.PlacementRoute{GrainKey: key2, NodeIdentity: "game/default/game-2"})
	cache.SetCachedPlacement(key3, sp.PlacementRoute{GrainKey: key3, NodeIdentity: "game/shanghai/game-1"})
	cache.DeleteCachedPlacementsByGroup("game", "default")

	if _, ok := cache.GetCachedPlacement(key1); ok {
		t.Fatal("default group key1 survived delete")
	}
	if _, ok := cache.GetCachedPlacement(key2); ok {
		t.Fatal("default group key2 survived delete")
	}
	if _, ok := cache.GetCachedPlacement(key3); !ok {
		t.Fatal("other group was deleted")
	}
}
