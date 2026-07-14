package stableplacement

import (
	"errors"
	"testing"
	"time"
)

func TestStrategyModeConstants(t *testing.T) {
	if StrategyModeGo != "go" || StrategyModeRedisRoundRobin != "redis_round_robin" || StrategyModeRedisResourceAware != "redis_resource_aware" {
		t.Fatalf("unexpected strategy modes: %q %q %q", StrategyModeGo, StrategyModeRedisRoundRobin, StrategyModeRedisResourceAware)
	}
}

func TestNormalizeResourceAwareConfig(t *testing.T) {
	config, err := NormalizeResourceAwareConfig(ResourceAwareConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if config.MetricsMaxAge != 10*time.Second || config.MinMemoryAvailableBytes != 256<<20 || config.MinCPUAvailableMilliCores != 100 || config.Now == nil {
		t.Fatalf("resource config = %+v", config)
	}
	if _, err := NormalizeResourceAwareConfig(ResourceAwareConfig{MaxGoroutines: -1}); !errors.Is(err, ErrPlacementConfigInvalid) {
		t.Fatalf("negative max goroutines error = %v", err)
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
