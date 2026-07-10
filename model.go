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
	// GrainID 是业务实体 ID。
	GrainID string
	// Kind 是业务实体类型。
	Kind string
	// TargetNodeType 是该 Grain 期望分配到的节点类型。
	TargetNodeType string
	// TargetNodeGroup 是该 Grain 期望分配到的节点分组。
	TargetNodeGroup string
}

type Node struct {
	// NodeType 是节点类型，例如 game、chat。
	NodeType string
	// NodeGroup 是节点分组，例如 default、world。
	NodeGroup string
	// NodeName 是同一 NodeType + NodeGroup 下的实例名。
	NodeName string
	// NodeIdentity 是稳定节点身份，格式为 NodeType/NodeGroup/NodeName。
	NodeIdentity string
	// NodeSessionID 是一次具体运行实例的 ID。
	NodeSessionID string
	// Address 是节点通信地址。
	Address string
	// Weight 是分配权重，供策略使用。
	Weight int
	// Load 是节点当前负载，供策略或观测使用。
	Load int
	// Status 是节点状态。
	Status NodeStatus
	// LastHeartbeatAt 是节点最后心跳时间。
	LastHeartbeatAt time.Time
}

type Lease struct {
	// OwnerNodeIdentity 是当前租约 Owner 的稳定节点身份。
	OwnerNodeIdentity string
	// OwnerNodeSessionID 是当前租约 Owner 的运行实例 ID。
	OwnerNodeSessionID string
	// Version 是租约版本，用于并发校验。
	Version int64
	// ExpireAt 是租约过期时间。
	ExpireAt time.Time
}

type Placement struct {
	// GrainID 是业务实体 ID。
	GrainID string
	// Kind 是业务实体类型。
	Kind string
	// GrainKey 是 Kind + GrainID 组成的唯一键。
	GrainKey GrainKey
	// NodeIdentity 是当前 Owner 节点身份。
	NodeIdentity string
	// Version 是 Placement 版本，用于并发校验。
	Version int64
	// Status 是 Placement 当前状态。
	Status PlacementStatus
	// CreateTime 是 Placement 创建时间。
	CreateTime time.Time
	// UpdateTime 是 Placement 最近更新时间。
	UpdateTime time.Time
	// LeaseExpireAt 是当前租约过期时间的冗余快照，方便路由判断。
	LeaseExpireAt time.Time
	// Lease 是当前 Owner 的租约信息。
	Lease Lease
}

// PlacementRecoverable 判断 Placement 当前是否允许 Recover。
// Released 表示业务主动结束，不允许 Recover；Active 与 Expired 属于故障恢复场景。
func PlacementRecoverable(status PlacementStatus) bool {
	return status == PlacementStatusActive || status == PlacementStatusExpired
}

type PlacementRoute struct {
	// GrainKey 是缓存路由对应的 Grain。
	GrainKey GrainKey
	// NodeIdentity 是缓存路由指向的 Owner 节点。
	NodeIdentity string
	// Version 是缓存时看到的 Placement 版本。
	Version int64
	// Status 是缓存时看到的 Placement 状态。
	Status PlacementStatus
	// LeaseExpireAt 是缓存时看到的租约过期时间。
	LeaseExpireAt time.Time
}
