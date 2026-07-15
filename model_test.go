package stableplacement

import (
	"encoding/hex"
	"errors"
	"testing"
	"time"
)

func TestStatusesAndErrorsAreStable(t *testing.T) {
	if NodeStatusActive != "active" || NodeStatusDraining != "draining" || NodeStatusOffline != "offline" {
		t.Fatalf("unexpected node statuses")
	}
	if PlacementStatusActive != "active" || PlacementStatusReleased != "released" {
		t.Fatalf("unexpected placement statuses")
	}
	if !errors.Is(ErrInvalidNodeSession, ErrInvalidNodeSession) {
		t.Fatal("errors.Is failed for ErrInvalidNodeSession")
	}
}

func TestDefaultNodeLeaseConfig(t *testing.T) {
	config := DefaultNodeLeaseConfig()
	if config.TTL != DefaultNodeLeaseTTL {
		t.Fatalf("default node lease TTL = %v, want %v", config.TTL, DefaultNodeLeaseTTL)
	}
}

func TestNewPlacementIDIsUniqueAndNonEmpty(t *testing.T) {
	first, err := NewPlacementID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPlacementID()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != hex.EncodedLen(PlacementIDByteLength) || len(second) != hex.EncodedLen(PlacementIDByteLength) || first == second {
		t.Fatalf("placement IDs = %q / %q", first, second)
	}
}

func TestNodeLeaseAndPlacementRouteContracts(t *testing.T) {
	grant := NodeLeaseGrant{Version: 3, ValidUntil: time.Unix(100, 0)}
	if grant.Version != 3 || grant.ValidUntil.IsZero() {
		t.Fatalf("node lease grant = %+v", grant)
	}
	lease := NodeLease{
		Version:           3,
		TTLMillis:         60_000,
		ExpireAtUnixMilli: 1_725_000_000_000,
	}
	node := Node{Lease: lease}
	if node.Lease != lease {
		t.Fatalf("node lease = %+v, want %+v", node.Lease, lease)
	}

	key, err := NewGrainKey("Player", "10001")
	if err != nil {
		t.Fatalf("NewGrainKey error: %v", err)
	}
	placement := Placement{GrainKey: key, PlacementID: "placement-1", OwnerNodeSessionID: "session-a"}
	if placement.OwnerNodeSessionID != "session-a" {
		t.Fatalf("placement owner session = %q", placement.OwnerNodeSessionID)
	}

	validUntil := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
	route := PlacementRoute{
		GrainKey:           key,
		PlacementID:        placement.PlacementID,
		OwnerNodeSessionID: "session-a",
		NodeLeaseVersion:   lease.Version,
		ValidUntil:         validUntil,
	}
	if route.PlacementID != placement.PlacementID || route.OwnerNodeSessionID != placement.OwnerNodeSessionID || route.NodeLeaseVersion != lease.Version || route.ValidUntil != validUntil {
		t.Fatalf("unexpected placement route: %+v", route)
	}
}

func TestNodeMetricsSurviveNodeCopy(t *testing.T) {
	want := NodeMetrics{CPUAvailableMilliCores: 500, MemoryAvailableBytes: 1 << 30, Goroutines: 8, UpdatedAtUnixMilli: 1234}
	node := Node{Metrics: want}
	copied := node
	if copied.Metrics != want {
		t.Fatalf("node metrics = %+v, want %+v", copied.Metrics, want)
	}
}

func TestValidateNodeMetricsRejectsNegativeCollectedValues(t *testing.T) {
	if err := ValidateNodeMetrics(NodeMetrics{CPUAvailableMilliCores: 1, MemoryAvailableBytes: 1, Goroutines: 1, UpdatedAtUnixMilli: -1}); err != nil {
		t.Fatalf("registry-owned timestamp should be ignored: %v", err)
	}
	if err := ValidateNodeMetrics(NodeMetrics{MemoryAvailableBytes: -1}); !errors.Is(err, ErrPlacementConfigInvalid) {
		t.Fatalf("negative metrics error = %v", err)
	}
}
