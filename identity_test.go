package stableplacement

import "testing"

func TestIdentityBuildAndParse(t *testing.T) {
	key, err := NewGrainKey("Player", "10001")
	if err != nil {
		t.Fatalf("NewGrainKey error: %v", err)
	}
	if key.String() != "Player/10001" {
		t.Fatalf("grain key = %q", key.String())
	}

	id, err := NewNodeIdentity("game", "default", "game-1")
	if err != nil {
		t.Fatalf("NewNodeIdentity error: %v", err)
	}
	if id.String() != "game/default/game-1" {
		t.Fatalf("node identity = %q", id.String())
	}
	if id.NodeType() != "game" || id.NodeGroup() != "default" || id.NodeName() != "game-1" {
		t.Fatalf("parsed identity = %s/%s/%s", id.NodeType(), id.NodeGroup(), id.NodeName())
	}
}

func TestIdentityRejectsEmptyParts(t *testing.T) {
	if _, err := NewGrainKey("", "10001"); err == nil {
		t.Fatal("NewGrainKey accepted empty kind")
	}
	if _, err := NewNodeIdentity("game", "", "game-1"); err == nil {
		t.Fatal("NewNodeIdentity accepted empty group")
	}
}
