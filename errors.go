package stableplacement

import "errors"

var (
	ErrPlacementNotFound       = errors.New("placement not found")
	ErrPlacementNotRecoverable = errors.New("placement not recoverable")
	ErrNoAvailableNode         = errors.New("no available node")
	ErrInvalidOwner            = errors.New("invalid owner")
	ErrInvalidNodeSession      = errors.New("invalid node session")
	ErrVersionConflict         = errors.New("version conflict")
	ErrLeaseExpired            = errors.New("lease expired")
	ErrLeaseNotExpired         = errors.New("lease not expired")
	ErrNodeNotFound            = errors.New("node not found")
	ErrNodeHasPlacements       = errors.New("node still has active placements")
	ErrNodeNotInvalid          = errors.New("node is not marked invalid")
	ErrUnsupportedStrategyMode = errors.New("unsupported strategy mode")
)
