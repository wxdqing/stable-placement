package stableplacement

import (
	"errors"
	"testing"
)

func TestStrategyModeConstants(t *testing.T) {
	if StrategyModeGo != "go" || StrategyModeRedisRoundRobin != "redis_round_robin" {
		t.Fatalf("unexpected strategy modes: %q %q", StrategyModeGo, StrategyModeRedisRoundRobin)
	}
}

func TestPlacementRecoverable(t *testing.T) {
	if !PlacementRecoverable(PlacementStatusActive) {
		t.Fatal("active placement should be recoverable")
	}
	if PlacementRecoverable(PlacementStatusReleased) {
		t.Fatal("released placement should not be recoverable")
	}
}

func TestRecoverableErrors(t *testing.T) {
	if !errors.Is(ErrPlacementNotRecoverable, ErrPlacementNotRecoverable) {
		t.Fatal("errors.Is failed for ErrPlacementNotRecoverable")
	}
	if !errors.Is(ErrUnsupportedStrategyMode, ErrUnsupportedStrategyMode) {
		t.Fatal("errors.Is failed for ErrUnsupportedStrategyMode")
	}
}
