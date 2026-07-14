package stableplacement

import (
	"fmt"
	"time"
)

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
)

type NodeLease struct {
	// Version 是 Node Lease 版本，用于并发校验。
	Version int64
	// TTLMillis 是该 NodeSession 注册时固定的租约时长。
	TTLMillis int64
	// ExpireAtUnixMilli 是 Node Lease 的绝对到期时间。
	ExpireAtUnixMilli int64
}

type NodeLeaseConfig struct {
	TTL time.Duration
}

type NodeLeaseGrant struct {
	Version    int64
	ValidUntil time.Time
}

type NodeMetrics struct {
	CPUAvailableMilliCores int64
	MemoryAvailableBytes   int64
	Goroutines             int64
	UpdatedAtUnixMilli     int64
}

func ValidateNodeMetrics(metrics NodeMetrics) error {
	if metrics.CPUAvailableMilliCores < 0 || metrics.MemoryAvailableBytes < 0 || metrics.Goroutines < 0 {
		return fmt.Errorf("%w: node metrics must not be negative", ErrPlacementConfigInvalid)
	}
	return nil
}

func DefaultNodeLeaseConfig() NodeLeaseConfig {
	return NodeLeaseConfig{TTL: time.Minute}
}

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
	// Metrics is the latest resource snapshot accepted during lease renewal.
	Metrics NodeMetrics
	// Status 是节点状态。
	Status NodeStatus
	// Lease 是当前 NodeSession 的租约。
	Lease NodeLease
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
	// OwnerNodeSessionID 是分配时的 Owner 运行实例快照。
	OwnerNodeSessionID string
	// Version 是 Placement 版本，用于并发校验。
	Version int64
	// Status 是 Placement 当前状态。
	Status PlacementStatus
	// CreateTime 是 Placement 创建时间。
	CreateTime time.Time
	// UpdateTime 是 Placement 最近更新时间。
	UpdateTime time.Time
}

// PlacementRecoverable 判断 Placement 当前是否允许 Recover。
// Released 表示业务主动结束，不允许 Recover；Active 可在 Owner 不可用时恢复。
func PlacementRecoverable(status PlacementStatus) bool {
	return status == PlacementStatusActive
}

type PlacementRoute struct {
	// GrainKey 是缓存路由对应的 Grain。
	GrainKey GrainKey
	// NodeIdentity 是缓存路由指向的 Owner 节点。
	NodeIdentity string
	// OwnerNodeSessionID 是缓存路由指向的 Owner 运行实例。
	OwnerNodeSessionID string
	// Version 是缓存时看到的 Placement 版本。
	Version int64
	// Status 是缓存时看到的 Placement 状态。
	Status PlacementStatus
	// NodeLeaseVersion 是缓存时看到的 Node Lease 版本。
	NodeLeaseVersion int64
	// ValidUntil 是当前进程可使用该路由的保守截止时间。
	ValidUntil time.Time
}
