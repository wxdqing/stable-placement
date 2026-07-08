package stableplacement

import "time"

type NodeStatus string

const (
	NodeStatusActive   NodeStatus = "active"
	NodeStatusDraining NodeStatus = "draining"
	NodeStatusOffline  NodeStatus = "offline"
)

type PlacementStatus string

const (
	PlacementStatusActive   PlacementStatus = "active"
	PlacementStatusReleased PlacementStatus = "released"
	PlacementStatusExpired  PlacementStatus = "expired"
)

type Grain struct {
	GrainID         string
	Kind            string
	TargetNodeType  string
	TargetNodeGroup string
}

type Node struct {
	NodeType        string
	NodeGroup       string
	NodeName        string
	NodeIdentity    string
	NodeSessionID   string
	Address         string
	Weight          int
	Load            int
	Status          NodeStatus
	LastHeartbeatAt time.Time
}

type Lease struct {
	OwnerNodeIdentity  string
	OwnerNodeSessionID string
	Version            int64
	ExpireAt           time.Time
}

type Placement struct {
	GrainID       string
	Kind          string
	GrainKey      GrainKey
	NodeIdentity  string
	Version       int64
	Status        PlacementStatus
	CreateTime    time.Time
	UpdateTime    time.Time
	LeaseExpireAt time.Time
	Lease         Lease
}

type PlacementRoute struct {
	GrainKey      GrainKey
	NodeIdentity  string
	Version       int64
	Status        PlacementStatus
	LeaseExpireAt time.Time
}
