package stableplacement

import (
	"context"
	"testing"
)

type directoryV2 interface {
	Lookup(context.Context, GrainKey) (*PlacementRoute, error)
}

type nodeRegistryV2 interface {
	ExpireNodeLeases(context.Context, string, string, int64) (int, error)
}

var _ directoryV2 = (Directory)(nil)
var _ nodeRegistryV2 = (NodeRegistry)(nil)

func TestCommandTypesCarryOwnerSessionAndVersions(t *testing.T) {
	key, _ := NewGrainKey("Player", "10001")
	renew := RenewCommand{
		GrainKey:         key,
		NodeIdentity:     "game/default/game-1",
		NodeSessionID:    "session-a",
		PlacementVersion: 2,
	}
	if renew.NodeIdentity == "" || renew.NodeSessionID == "" || renew.PlacementVersion == 0 {
		t.Fatalf("renew command lost owner or version fields: %+v", renew)
	}

	release := ReleaseCommand{
		GrainKey:         key,
		NodeIdentity:     renew.NodeIdentity,
		NodeSessionID:    renew.NodeSessionID,
		PlacementVersion: renew.PlacementVersion,
	}
	if release.NodeIdentity != renew.NodeIdentity || release.NodeSessionID != renew.NodeSessionID {
		t.Fatalf("release command owner mismatch: %+v", release)
	}
}
