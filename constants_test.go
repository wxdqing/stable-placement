package stableplacement

import (
	"errors"
	"testing"
	"time"
)

func TestNormalizeNodeLeaseConfigRoundsUpToMilliseconds(t *testing.T) {
	config, err := NormalizeNodeLeaseConfig(NodeLeaseConfig{TTL: time.Microsecond})
	if err != nil {
		t.Fatal(err)
	}
	if config.TTL != time.Millisecond {
		t.Fatalf("normalized TTL = %v, want %v", config.TTL, time.Millisecond)
	}
	if _, err := NormalizeNodeLeaseConfig(NodeLeaseConfig{}); !errors.Is(err, ErrInvalidNodeLeaseTTL) {
		t.Fatalf("zero TTL error = %v", err)
	}
}
