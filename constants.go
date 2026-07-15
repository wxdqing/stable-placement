package stableplacement

import "time"

const (
	MaxIdentityPartBytes = 128
	MaxGrainIDBytes      = 256

	PlacementIDByteLength = 16

	InitialPlacementVersion int64 = 1
	InitialNodeLeaseVersion int64 = 1
	DefaultNodeLeaseTTL           = time.Minute
	MaxNodeLeaseTTLMillis   int64 = int64(^uint64(0)>>1) / int64(time.Millisecond)

	DefaultPlacementPageLimit = 100
)

func NormalizeNodeLeaseConfig(config NodeLeaseConfig) (NodeLeaseConfig, error) {
	if config.TTL <= 0 {
		return NodeLeaseConfig{}, ErrInvalidNodeLeaseTTL
	}
	ttlMillis := config.TTL.Milliseconds()
	if config.TTL%time.Millisecond != 0 && ttlMillis < MaxNodeLeaseTTLMillis {
		ttlMillis++
	}
	config.TTL = time.Duration(ttlMillis) * time.Millisecond
	return config, nil
}
