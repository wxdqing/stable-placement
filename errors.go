package stableplacement

import "errors"

var (
	ErrPlacementNotFound  = errors.New("placement not found")
	ErrPlacementExists    = errors.New("placement already exists")
	ErrNoAvailableNode    = errors.New("no available node")
	ErrInvalidOwner       = errors.New("invalid owner")
	ErrInvalidNodeSession = errors.New("invalid node session")
	ErrVersionConflict    = errors.New("version conflict")
	ErrLeaseExpired       = errors.New("lease expired")
	ErrLeaseNotExpired    = errors.New("lease not expired")
	ErrNodeNotFound       = errors.New("node not found")
	ErrNodeNotInvalid     = errors.New("node is not marked invalid")
)
