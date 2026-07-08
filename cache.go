package stableplacement

type PlacementCache interface {
	GetCachedPlacement(key GrainKey) (*PlacementRoute, bool)
	SetCachedPlacement(key GrainKey, route PlacementRoute)
	DeleteCachedPlacement(key GrainKey)
	DeleteCachedPlacementsByNode(nodeIdentity string)
	DeleteCachedPlacementsByGroup(nodeType string, nodeGroup string)
	ClearPlacementCache()
	SetDegraded(degraded bool)
	IsDegraded() bool
}
