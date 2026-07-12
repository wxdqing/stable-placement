package stableplacement

import (
	"strings"
	"testing"
)

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

func TestNewGrainKeyUsesExactCase(t *testing.T) {
	lower, err := NewGrainKey("player", "acct-1")
	if err != nil {
		t.Fatalf("NewGrainKey lower: %v", err)
	}
	upper, err := NewGrainKey("Player", "acct-1")
	if err != nil {
		t.Fatalf("NewGrainKey upper: %v", err)
	}
	if lower == upper || lower != "player/acct-1" {
		t.Fatalf("keys = %q, %q", lower, upper)
	}
}

func TestIdentityRejectsInvalidParts(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "leading whitespace", value: " game"},
		{name: "trailing whitespace", value: "game "},
		{name: "separator", value: "ga/me"},
		{name: "nul", value: "ga\x00me"},
		{name: "control", value: "ga\nme"},
		{name: "invalid utf8", value: string([]byte{0xff})},
		{name: "too long", value: strings.Repeat("x", 129)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewNodeIdentity(test.value, "default", "game-1"); err == nil {
				t.Fatalf("NewNodeIdentity accepted %q", test.value)
			}
			if _, err := NewGrainKey(test.value, "acct-1"); err == nil {
				t.Fatalf("NewGrainKey kind accepted %q", test.value)
			}
		})
	}
}

func TestIdentityLengthBoundaries(t *testing.T) {
	if _, err := NewNodeIdentity(strings.Repeat("n", 128), "default", "game-1"); err != nil {
		t.Fatalf("128-byte node type rejected: %v", err)
	}
	if _, err := NewGrainKey("player", strings.Repeat("a", 256)); err != nil {
		t.Fatalf("256-byte grain id rejected: %v", err)
	}
	if _, err := NewGrainKey("player", strings.Repeat("a", 257)); err == nil {
		t.Fatal("257-byte grain id accepted")
	}
}
