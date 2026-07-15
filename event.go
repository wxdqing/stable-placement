package stableplacement

import (
	"context"
	"time"
)

type EventType string

const (
	EventNodeRegistered            EventType = "NodeRegistered"
	EventNodeReplaced              EventType = "NodeReplaced"
	EventNodeDraining              EventType = "NodeDraining"
	EventNodeMarkedInvalid         EventType = "NodeMarkedInvalid"
	EventNodeRestored              EventType = "NodeRestored"
	EventNodeUnregistered          EventType = "NodeUnregistered"
	EventPlacementCreated          EventType = "PlacementCreated"
	EventPlacementRenewed          EventType = "PlacementRenewed"
	EventPlacementReleased         EventType = "PlacementReleased"
	EventPlacementTransferred      EventType = "PlacementTransferred"
	EventPlacementRecovered        EventType = "PlacementRecovered"
	EventNodeLeaseExpired          EventType = "NodeLeaseExpired"
	EventPlacementCacheInvalidated EventType = "PlacementCacheInvalidated"
	EventManualCacheClear          EventType = "ManualCacheClear"
)

type PlacementEvent struct {
	// Type 是事件类型。
	Type EventType
	// GrainKey 是事件影响的 Grain；节点事件可以为空。
	GrainKey GrainKey
	// PlacementID identifies the allocation lifetime affected by the event.
	PlacementID string
	// NodeIdentity 是事件影响的完整节点身份。
	NodeIdentity string
	// NodeSessionID 是事件影响的节点运行实例 ID。
	NodeSessionID string
	// NodeType 是事件影响的节点类型。
	NodeType string
	// NodeGroup 是事件影响的节点分组。
	NodeGroup string
	// NodeName 是事件影响的节点实例名。
	NodeName string
	// PlacementVersion 是事件发生后的 Placement 版本。
	PlacementVersion int64
	// NodeLeaseVersion 是事件发生后的 Node Lease 版本。
	NodeLeaseVersion int64
	// Time 是事件发生时间。
	Time time.Time
}

type EventPublisher interface {
	Publish(ctx context.Context, event PlacementEvent) error
}

type EventSubscriber interface {
	Subscribe(ctx context.Context, handler func(PlacementEvent) error) error
}
