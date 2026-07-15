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
		PlacementID:      "placement-1",
		NodeIdentity:     "game/default/game-1",
		NodeSessionID:    "session-a",
		PlacementVersion: 2,
	}
	if renew.PlacementID == "" || renew.NodeIdentity == "" || renew.NodeSessionID == "" || renew.PlacementVersion == 0 {
		t.Fatalf("renew command lost owner or version fields: %+v", renew)
	}

	release := ReleaseCommand{
		GrainKey:         key,
		PlacementID:      renew.PlacementID,
		NodeIdentity:     renew.NodeIdentity,
		NodeSessionID:    renew.NodeSessionID,
		PlacementVersion: renew.PlacementVersion,
	}
	if release.PlacementID != renew.PlacementID || release.NodeIdentity != renew.NodeIdentity || release.NodeSessionID != renew.NodeSessionID {
		t.Fatalf("release command owner mismatch: %+v", release)
	}
}

func TestRenewNodeCommandCarriesOptionalMetrics(t *testing.T) {
	metrics := NodeMetrics{CPUAvailableMilliCores: 250, MemoryAvailableBytes: 512 << 20, Goroutines: 4}
	cmd := RenewNodeCommand{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a", Metrics: &metrics}
	if cmd.NodeIdentity == "" || cmd.NodeSessionID == "" || cmd.Metrics == nil || *cmd.Metrics != metrics {
		t.Fatalf("renew node command = %+v", cmd)
	}
}
