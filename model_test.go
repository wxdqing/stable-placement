package stableplacement

import (
	"errors"
	"testing"
)

func TestStatusesAndErrorsAreStable(t *testing.T) {
	if NodeStatusActive != "active" || NodeStatusDraining != "draining" || NodeStatusOffline != "offline" {
		t.Fatalf("unexpected node statuses")
	}
	if PlacementStatusActive != "active" || PlacementStatusReleased != "released" || PlacementStatusExpired != "expired" {
		t.Fatalf("unexpected placement statuses")
	}
	if !errors.Is(ErrInvalidNodeSession, ErrInvalidNodeSession) {
		t.Fatal("errors.Is failed for ErrInvalidNodeSession")
	}
}
