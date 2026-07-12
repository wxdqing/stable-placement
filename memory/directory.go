package memory

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

type Directory struct {
	mu         sync.RWMutex
	placements map[sp.GrainKey]sp.Placement
	byNode     map[string]map[sp.GrainKey]struct{}
	registry   *NodeRegistry
	strategy   sp.PlacementStrategy
	publisher  sp.EventPublisher
}

func NewDirectory(registry *NodeRegistry, mode sp.StrategyMode, strategy sp.PlacementStrategy, publisher sp.EventPublisher) (*Directory, error) {
	if mode != sp.StrategyModeGo {
		return nil, sp.ErrUnsupportedStrategyMode
	}
	d := &Directory{
		placements: make(map[sp.GrainKey]sp.Placement),
		byNode:     make(map[string]map[sp.GrainKey]struct{}),
		registry:   registry,
		strategy:   strategy,
		publisher:  publisher,
	}
	registry.completeDrain = func(nodeIdentity string, nodeSessionID string) (sp.PlacementEvent, error) {
		d.mu.Lock()
		defer d.mu.Unlock()
		return registry.unregisterNode(nodeIdentity, nodeSessionID, func() bool {
			return len(d.byNode[nodeIdentity]) > 0
		})
	}
	return d, nil
}

func (d *Directory) NodeRegistry() *NodeRegistry { return d.registry }

func (d *Directory) Lookup(_ context.Context, key sp.GrainKey) (*sp.PlacementRoute, error) {
	d.mu.RLock()
	d.registry.mu.RLock()
	now := d.registry.now()
	placement, ok := d.placements[key]
	if !ok || !d.placementRouteableLocked(placement, now) {
		d.registry.mu.RUnlock()
		d.mu.RUnlock()
		return nil, sp.ErrPlacementNotFound
	}
	node := d.registry.nodes[placement.NodeIdentity]
	route := sp.PlacementRoute{
		GrainKey:           placement.GrainKey,
		NodeIdentity:       placement.NodeIdentity,
		OwnerNodeSessionID: placement.OwnerNodeSessionID,
		Version:            placement.Version,
		Status:             placement.Status,
		NodeLeaseVersion:   node.Lease.Version,
		ValidUntil:         now.Add(time.Duration(node.Lease.ExpireAtUnixMilli-now.UnixMilli()) * time.Millisecond),
	}
	d.registry.mu.RUnlock()
	d.mu.RUnlock()
	return &route, nil
}

func (d *Directory) Allocate(ctx context.Context, cmd sp.AllocateCommand) (*sp.Placement, error) {
	key, err := sp.NewGrainKey(cmd.Kind, cmd.GrainID)
	if err != nil {
		return nil, err
	}

	if placement, found, err := d.existingAllocation(key); found || err != nil {
		return placement, err
	}
	nodes := d.effectiveNodes(cmd.TargetNodeType, cmd.TargetNodeGroup)
	if len(nodes) == 0 {
		return nil, sp.ErrNoAvailableNode
	}
	chosen, err := d.strategy.Choose(ctx, sp.StrategyInput{
		GrainID: cmd.GrainID, Kind: cmd.Kind, NodeType: cmd.TargetNodeType, NodeGroup: cmd.TargetNodeGroup, EffectiveNodes: nodes,
	})
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.registry.mu.RLock()
	now := d.registry.now()
	if existing, ok := d.placements[key]; ok && existing.Status == sp.PlacementStatusActive {
		if d.placementRouteableLocked(existing, now) {
			d.registry.mu.RUnlock()
			d.mu.Unlock()
			return copyPlacement(existing), nil
		}
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrPlacementOwnerUnavailable
	}
	current, ok := d.registry.nodes[chosen.NodeIdentity]
	if !ok || current.NodeSessionID != chosen.NodeSessionID || current.NodeType != cmd.TargetNodeType || current.NodeGroup != cmd.TargetNodeGroup || !d.targetAvailableLocked(current, now) {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrNoAvailableNode
	}
	version := int64(1)
	if existing, ok := d.placements[key]; ok {
		version = existing.Version + 1
		d.deleteNodeIndexLocked(existing.NodeIdentity, key)
	}
	placement := sp.Placement{
		GrainID: cmd.GrainID, Kind: cmd.Kind, GrainKey: key, NodeIdentity: current.NodeIdentity,
		OwnerNodeSessionID: current.NodeSessionID, Version: version, Status: sp.PlacementStatusActive,
		CreateTime: now, UpdateTime: now,
	}
	d.placements[key] = placement
	d.addNodeIndexLocked(current.NodeIdentity, key)
	d.registry.mu.RUnlock()
	d.mu.Unlock()

	_ = d.publish(ctx, placement, sp.EventPlacementCreated)
	return copyPlacement(placement), nil
}

func (d *Directory) Renew(ctx context.Context, cmd sp.RenewCommand) (*sp.Placement, error) {
	d.mu.RLock()
	d.registry.mu.RLock()
	now := d.registry.now()
	placement, ok := d.placements[cmd.GrainKey]
	if !ok || placement.Status != sp.PlacementStatusActive {
		d.registry.mu.RUnlock()
		d.mu.RUnlock()
		return nil, sp.ErrPlacementNotFound
	}
	if placement.NodeIdentity != cmd.NodeIdentity {
		d.registry.mu.RUnlock()
		d.mu.RUnlock()
		return nil, sp.ErrInvalidOwner
	}
	if placement.Version != cmd.PlacementVersion {
		d.registry.mu.RUnlock()
		d.mu.RUnlock()
		return nil, sp.ErrVersionConflict
	}
	node, exists := d.registry.nodes[placement.NodeIdentity]
	if !exists || node.Status == sp.NodeStatusOffline {
		d.registry.mu.RUnlock()
		d.mu.RUnlock()
		return nil, sp.ErrPlacementOwnerUnavailable
	}
	if placement.OwnerNodeSessionID != cmd.NodeSessionID || node.NodeSessionID != cmd.NodeSessionID {
		d.registry.mu.RUnlock()
		d.mu.RUnlock()
		return nil, sp.ErrInvalidNodeSession
	}
	if leaseExpired(node, now) {
		d.registry.mu.RUnlock()
		d.mu.RUnlock()
		return nil, sp.ErrNodeLeaseExpired
	}
	d.registry.mu.RUnlock()
	d.mu.RUnlock()

	if err := d.publish(ctx, placement, sp.EventPlacementRenewed); err != nil {
		return nil, err
	}
	return copyPlacement(placement), nil
}

func (d *Directory) Release(ctx context.Context, cmd sp.ReleaseCommand) error {
	d.mu.Lock()
	d.registry.mu.RLock()
	placement, ok := d.placements[cmd.GrainKey]
	if !ok || placement.Status != sp.PlacementStatusActive {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return sp.ErrPlacementNotFound
	}
	if placement.NodeIdentity != cmd.NodeIdentity {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return sp.ErrInvalidOwner
	}
	if placement.Version != cmd.PlacementVersion {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return sp.ErrVersionConflict
	}
	node, exists := d.registry.nodes[placement.NodeIdentity]
	if !exists {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return sp.ErrPlacementOwnerUnavailable
	}
	if placement.OwnerNodeSessionID != cmd.NodeSessionID || node.NodeSessionID != cmd.NodeSessionID {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return sp.ErrInvalidNodeSession
	}
	placement.Version++
	placement.Status = sp.PlacementStatusReleased
	placement.UpdateTime = d.registry.now()
	d.placements[cmd.GrainKey] = placement
	d.deleteNodeIndexLocked(placement.NodeIdentity, cmd.GrainKey)
	d.registry.mu.RUnlock()
	d.mu.Unlock()
	return d.publish(ctx, placement, sp.EventPlacementReleased)
}

func (d *Directory) Transfer(ctx context.Context, cmd sp.TransferCommand) (*sp.Placement, error) {
	d.mu.Lock()
	d.registry.mu.RLock()
	now := d.registry.now()
	placement, ok := d.placements[cmd.GrainKey]
	if !ok || placement.Status != sp.PlacementStatusActive {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrPlacementNotFound
	}
	if placement.Version != cmd.PlacementVersion {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrVersionConflict
	}
	if cmd.FromNodeIdentity != "" && placement.NodeIdentity != cmd.FromNodeIdentity {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrInvalidOwner
	}
	target, ok := d.registry.nodes[cmd.ToNodeIdentity]
	if !ok || !d.targetAvailableLocked(target, now) {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrNoAvailableNode
	}
	d.movePlacementLocked(&placement, target, now)
	d.registry.mu.RUnlock()
	d.mu.Unlock()
	_ = d.publish(ctx, placement, sp.EventPlacementTransferred)
	return copyPlacement(placement), nil
}

func (d *Directory) Recover(ctx context.Context, cmd sp.RecoverCommand) (*sp.Placement, error) {
	d.mu.Lock()
	d.registry.mu.RLock()
	now := d.registry.now()
	placement, ok := d.placements[cmd.GrainKey]
	if !ok {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrPlacementNotFound
	}
	if placement.Version != cmd.PlacementVersion {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrVersionConflict
	}
	if !sp.PlacementRecoverable(placement.Status) || d.placementRouteableLocked(placement, now) {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrPlacementNotRecoverable
	}
	target, ok := d.registry.nodes[cmd.NewNodeIdentity]
	if !ok || !d.targetAvailableLocked(target, now) {
		d.registry.mu.RUnlock()
		d.mu.Unlock()
		return nil, sp.ErrNoAvailableNode
	}
	d.movePlacementLocked(&placement, target, now)
	d.registry.mu.RUnlock()
	d.mu.Unlock()
	_ = d.publish(ctx, placement, sp.EventPlacementRecovered)
	return copyPlacement(placement), nil
}

func (d *Directory) Exists(_ context.Context, key sp.GrainKey) (bool, error) {
	d.mu.RLock()
	d.registry.mu.RLock()
	placement, ok := d.placements[key]
	exists := ok && d.placementRouteableLocked(placement, d.registry.now())
	d.registry.mu.RUnlock()
	d.mu.RUnlock()
	return exists, nil
}

func (d *Directory) FindByNode(_ context.Context, query sp.FindByNodeQuery) (sp.PlacementPage, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	status := query.Status
	if status == "" {
		status = sp.PlacementStatusActive
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	start := 0
	if query.Cursor != "" {
		parsed, err := strconv.Atoi(query.Cursor)
		if err != nil {
			return sp.PlacementPage{}, err
		}
		if parsed < 0 {
			return sp.PlacementPage{}, fmt.Errorf("cursor must not be negative")
		}
		start = parsed
	}
	var keys []string
	for key := range d.byNode[query.NodeIdentity] {
		keys = append(keys, key.String())
	}
	sort.Strings(keys)
	if start >= len(keys) {
		return sp.PlacementPage{}, nil
	}
	var placements []sp.Placement
	nextIndex := start
	for i := start; i < len(keys) && len(placements) < limit; i++ {
		placement := d.placements[sp.GrainKey(keys[i])]
		if placement.Status == status {
			placements = append(placements, placement)
		}
		nextIndex = i + 1
	}
	nextCursor := ""
	if nextIndex < len(keys) {
		nextCursor = strconv.Itoa(nextIndex)
	}
	return sp.PlacementPage{Placements: placements, NextCursor: nextCursor}, nil
}

func (d *Directory) existingAllocation(key sp.GrainKey) (*sp.Placement, bool, error) {
	d.mu.RLock()
	d.registry.mu.RLock()
	defer d.registry.mu.RUnlock()
	defer d.mu.RUnlock()
	placement, ok := d.placements[key]
	if !ok || placement.Status != sp.PlacementStatusActive {
		return nil, false, nil
	}
	if !d.placementRouteableLocked(placement, d.registry.now()) {
		return nil, true, sp.ErrPlacementOwnerUnavailable
	}
	return copyPlacement(placement), true, nil
}

func (d *Directory) effectiveNodes(nodeType, nodeGroup string) []sp.Node {
	d.registry.mu.RLock()
	defer d.registry.mu.RUnlock()
	now := d.registry.now()
	var nodes []sp.Node
	for _, node := range d.registry.nodes {
		if node.NodeType == nodeType && node.NodeGroup == nodeGroup && d.targetAvailableLocked(node, now) {
			nodes = append(nodes, node)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeIdentity < nodes[j].NodeIdentity })
	return nodes
}

func (d *Directory) placementRouteableLocked(placement sp.Placement, now time.Time) bool {
	if placement.Status != sp.PlacementStatusActive {
		return false
	}
	node, ok := d.registry.nodes[placement.NodeIdentity]
	return ok && placement.OwnerNodeSessionID == node.NodeSessionID &&
		(node.Status == sp.NodeStatusActive || node.Status == sp.NodeStatusDraining) && !leaseExpired(node, now)
}

func (d *Directory) targetAvailableLocked(node sp.Node, now time.Time) bool {
	return node.Status == sp.NodeStatusActive && !leaseExpired(node, now) &&
		!d.registry.isInvalidLocked(node.NodeType, node.NodeGroup, node.NodeName)
}

func (d *Directory) movePlacementLocked(placement *sp.Placement, target sp.Node, now time.Time) {
	d.deleteNodeIndexLocked(placement.NodeIdentity, placement.GrainKey)
	placement.NodeIdentity = target.NodeIdentity
	placement.OwnerNodeSessionID = target.NodeSessionID
	placement.Version++
	placement.Status = sp.PlacementStatusActive
	placement.UpdateTime = now
	d.placements[placement.GrainKey] = *placement
	d.addNodeIndexLocked(target.NodeIdentity, placement.GrainKey)
}

func (d *Directory) addNodeIndexLocked(nodeIdentity string, key sp.GrainKey) {
	if d.byNode[nodeIdentity] == nil {
		d.byNode[nodeIdentity] = make(map[sp.GrainKey]struct{})
	}
	d.byNode[nodeIdentity][key] = struct{}{}
}

func (d *Directory) deleteNodeIndexLocked(nodeIdentity string, key sp.GrainKey) {
	delete(d.byNode[nodeIdentity], key)
	if len(d.byNode[nodeIdentity]) == 0 {
		delete(d.byNode, nodeIdentity)
	}
}

func (d *Directory) publish(ctx context.Context, placement sp.Placement, eventType sp.EventType) error {
	if d.publisher == nil {
		return nil
	}
	return d.publisher.Publish(ctx, sp.PlacementEvent{
		Type: eventType, GrainKey: placement.GrainKey, NodeIdentity: placement.NodeIdentity,
		NodeSessionID: placement.OwnerNodeSessionID, PlacementVersion: placement.Version, Time: d.registry.now(),
	})
}

func copyPlacement(placement sp.Placement) *sp.Placement {
	copied := placement
	return &copied
}
