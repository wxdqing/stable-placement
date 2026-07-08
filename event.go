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
	EventLeaseExpired              EventType = "LeaseExpired"
	EventPlacementCacheInvalidated EventType = "PlacementCacheInvalidated"
	EventManualCacheClear          EventType = "ManualCacheClear"
)

type PlacementEvent struct {
	Type             EventType
	GrainKey         GrainKey
	NodeIdentity     string
	NodeType         string
	NodeGroup        string
	NodeName         string
	PlacementVersion int64
	LeaseVersion     int64
	Time             time.Time
}

type EventPublisher interface {
	Publish(ctx context.Context, event PlacementEvent) error
}

type EventSubscriber interface {
	Subscribe(ctx context.Context, handler func(PlacementEvent) error) error
}
