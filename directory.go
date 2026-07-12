package stableplacement

import "context"

type Directory interface {
	Lookup(ctx context.Context, key GrainKey) (*PlacementRoute, error)
	Allocate(ctx context.Context, cmd AllocateCommand) (*Placement, error)
	Renew(ctx context.Context, cmd RenewCommand) (*Placement, error)
	Release(ctx context.Context, cmd ReleaseCommand) error
	Transfer(ctx context.Context, cmd TransferCommand) (*Placement, error)
	Recover(ctx context.Context, cmd RecoverCommand) (*Placement, error)
	Exists(ctx context.Context, key GrainKey) (bool, error)
	FindByNode(ctx context.Context, query FindByNodeQuery) (PlacementPage, error)
}
