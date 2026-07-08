package stableplacement

import "time"

type AllocateCommand struct {
	// GrainID 是业务实体 ID，例如玩家 ID。
	GrainID string
	// Kind 是业务实体类型，例如 Player、Guild。
	Kind string
	// TargetNodeType 是目标节点类型，例如 game、chat。
	TargetNodeType string
	// TargetNodeGroup 是目标节点分组，例如 default、world。
	TargetNodeGroup string
	// LeaseTTL 是首次分配后 Owner 租约有效期。
	LeaseTTL time.Duration
}

type RenewCommand struct {
	// GrainKey 唯一标识要续约的 Grain。
	GrainKey GrainKey
	// NodeIdentity 是当前 Owner 的稳定节点身份。
	NodeIdentity string
	// NodeSessionID 是当前 Owner 的运行实例 ID。
	NodeSessionID string
	// PlacementVersion 是调用方看到的 Placement 版本。
	PlacementVersion int64
	// LeaseVersion 是调用方看到的 Lease 版本。
	LeaseVersion int64
	// ExtendTTL 是本次续约要延长的租约时间。
	ExtendTTL time.Duration
}

type ReleaseCommand struct {
	// GrainKey 唯一标识要释放的 Grain。
	GrainKey GrainKey
	// NodeIdentity 是当前 Owner 的稳定节点身份。
	NodeIdentity string
	// NodeSessionID 是当前 Owner 的运行实例 ID。
	NodeSessionID string
	// PlacementVersion 是调用方看到的 Placement 版本。
	PlacementVersion int64
	// LeaseVersion 是调用方看到的 Lease 版本。
	LeaseVersion int64
}

type TransferCommand struct {
	// GrainKey 唯一标识要迁移的 Grain。
	GrainKey GrainKey
	// FromNodeIdentity 是迁移前的 Owner，非空时必须和当前 Placement 匹配。
	FromNodeIdentity string
	// ToNodeIdentity 是迁移目标节点。
	ToNodeIdentity string
	// PlacementVersion 是调用方看到的 Placement 版本。
	PlacementVersion int64
	// LeaseTTL 是迁移后新 Owner 的租约有效期。
	LeaseTTL time.Duration
}

type RecoverCommand struct {
	// GrainKey 唯一标识要恢复的 Grain。
	GrainKey GrainKey
	// NewNodeIdentity 是恢复后的新 Owner。
	NewNodeIdentity string
	// PlacementVersion 是调用方看到的 Placement 版本。
	PlacementVersion int64
	// LeaseTTL 是恢复后新 Owner 的租约有效期。
	LeaseTTL time.Duration
}

type ExpireCommand struct {
	// GrainKey 唯一标识要过期清理的 Grain。
	GrainKey GrainKey
	// LeaseVersion 是调用方看到的 Lease 版本。
	LeaseVersion int64
	// Now 是执行过期判断的当前时间；为空时使用系统当前时间。
	Now time.Time
}

type FindByNodeQuery struct {
	// NodeIdentity 是要查询的完整节点身份。
	NodeIdentity string
	// Status 是 Placement 状态过滤条件；为空时默认查询 Active。
	Status PlacementStatus
	// Cursor 是分页游标；传入上一页返回的 NextCursor 可继续查询下一页。
	Cursor string
	// Limit 是本次最多返回的 Placement 数量。
	Limit int
}

type PlacementPage struct {
	// Placements 是当前页的查询结果。
	Placements []Placement
	// NextCursor 是下一页游标；为空表示没有更多结果。
	NextCursor string
}
