package stableplacement

import (
	"errors"
	"testing"
)

func TestBuildNodeGroup(t *testing.T) {
	routes := map[string]KindRouteConfig{
		"player": {NodeType: "game", NodeGroupPrefix: "server-", GroupIDLabel: "server_id"},
		"Guild":  {NodeType: "game", NodeGroupPrefix: "world-", GroupIDLabel: "world_id"},
	}

	target, err := BuildNodeGroup("player", map[string]string{"server_id": "1001"}, routes)
	if err != nil {
		t.Fatalf("BuildNodeGroup: %v", err)
	}
	if target.NodeType != "game" || target.NodeGroup != "server-1001" {
		t.Fatalf("target = %#v", target)
	}

	guild, err := BuildNodeGroup("Guild", map[string]string{"world_id": "7"}, routes)
	if err != nil {
		t.Fatalf("BuildNodeGroup guild: %v", err)
	}
	if guild.NodeGroup != "world-7" {
		t.Fatalf("guild target = %#v", guild)
	}
}

func TestBuildNodeGroupRejectsInvalidConfiguration(t *testing.T) {
	valid := map[string]KindRouteConfig{
		"player": {NodeType: "game", NodeGroupPrefix: "server-", GroupIDLabel: "server_id"},
	}
	tests := []struct {
		name   string
		kind   string
		labels map[string]string
		routes map[string]KindRouteConfig
	}{
		{name: "unknown kind", kind: "Player", labels: map[string]string{"server_id": "1"}, routes: valid},
		{name: "missing label", kind: "player", labels: map[string]string{}, routes: valid},
		{name: "empty label", kind: "player", labels: map[string]string{"server_id": ""}, routes: valid},
		{name: "separator", kind: "player", labels: map[string]string{"server_id": "1/2"}, routes: valid},
		{name: "blank node type", kind: "player", labels: map[string]string{"server_id": "1"}, routes: map[string]KindRouteConfig{"player": {GroupIDLabel: "server_id"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := BuildNodeGroup(test.kind, test.labels, test.routes)
			if !errors.Is(err, ErrPlacementConfigInvalid) {
				t.Fatalf("BuildNodeGroup error = %v", err)
			}
		})
	}
}
