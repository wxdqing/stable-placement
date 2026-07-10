package memory

import (
	"errors"
	"testing"

	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/strategies"
)

func TestNewDirectoryRejectsRedisStrategyMode(t *testing.T) {
	bus := NewEventBus()
	_, err := NewDirectory(NewNodeRegistry(bus), sp.StrategyModeRedisRoundRobin, strategies.NewRoundRobin(), bus)
	if err == nil {
		t.Fatal("NewDirectory accepted redis strategy mode")
	}
	if !errors.Is(err, sp.ErrUnsupportedStrategyMode) {
		t.Fatalf("err = %v, want ErrUnsupportedStrategyMode", err)
	}
}
