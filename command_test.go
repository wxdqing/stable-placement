package stableplacement

import (
	"testing"
	"time"
)

func TestCommandTypesCarryOwnerSessionAndVersions(t *testing.T) {
	key, _ := NewGrainKey("Player", "10001")
	renew := RenewCommand{
		GrainKey:         key,
		NodeIdentity:     "game/default/game-1",
		NodeSessionID:    "session-a",
		PlacementVersion: 2,
		LeaseVersion:     3,
		ExtendTTL:        time.Minute,
	}
	if renew.NodeIdentity == "" || renew.NodeSessionID == "" || renew.PlacementVersion == 0 || renew.LeaseVersion == 0 {
		t.Fatalf("renew command lost owner or version fields: %+v", renew)
	}

	release := ReleaseCommand{
		GrainKey:         key,
		NodeIdentity:     renew.NodeIdentity,
		NodeSessionID:    renew.NodeSessionID,
		PlacementVersion: renew.PlacementVersion,
		LeaseVersion:     renew.LeaseVersion,
	}
	if release.NodeIdentity != renew.NodeIdentity || release.NodeSessionID != renew.NodeSessionID {
		t.Fatalf("release command owner mismatch: %+v", release)
	}
}
