package stableplacement

import "errors"

var (
	ErrPlacementNotFound         = errors.New("placement not found")
	ErrPlacementNotRecoverable   = errors.New("placement not recoverable")
	ErrNoAvailableNode           = errors.New("no available node")
	ErrInvalidOwner              = errors.New("invalid owner")
	ErrInvalidNodeSession        = errors.New("invalid node session")
	ErrVersionConflict           = errors.New("version conflict")
	ErrNodeLeaseExpired          = errors.New("node lease expired")
	ErrInvalidNodeLeaseTTL       = errors.New("invalid node lease TTL")
	ErrPlacementOwnerUnavailable = errors.New("placement owner unavailable")
	ErrNodeNotFound              = errors.New("node not found")
	ErrNodeHasPlacements         = errors.New("node still has active placements")
	ErrNodeNotInvalid            = errors.New("node is not marked invalid")
	ErrUnsupportedStrategyMode   = errors.New("unsupported strategy mode")
	ErrPlacementConfigInvalid    = errors.New("placement config invalid")
)
