package stableplacement

import "time"

type AllocateCommand struct {
	GrainID         string
	Kind            string
	TargetNodeType  string
	TargetNodeGroup string
	LeaseTTL        time.Duration
}

type RenewCommand struct {
	GrainKey         GrainKey
	NodeIdentity     string
	NodeSessionID    string
	PlacementVersion int64
	LeaseVersion     int64
	ExtendTTL        time.Duration
}

type ReleaseCommand struct {
	GrainKey         GrainKey
	NodeIdentity     string
	NodeSessionID    string
	PlacementVersion int64
	LeaseVersion     int64
}

type TransferCommand struct {
	GrainKey         GrainKey
	FromNodeIdentity string
	ToNodeIdentity   string
	PlacementVersion int64
	LeaseTTL         time.Duration
}

type RecoverCommand struct {
	GrainKey         GrainKey
	NewNodeIdentity  string
	PlacementVersion int64
	LeaseTTL         time.Duration
}

type FindByNodeQuery struct {
	NodeIdentity string
	Status       PlacementStatus
	Cursor       string
	Limit        int
}

type PlacementPage struct {
	Placements []Placement
	NextCursor string
}
