package memory

import (
	"context"
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

func NewDirectory(registry *NodeRegistry, strategy sp.PlacementStrategy, publisher sp.EventPublisher) *Directory {
	return &Directory{
		placements: make(map[sp.GrainKey]sp.Placement),
		byNode:     make(map[string]map[sp.GrainKey]struct{}),
		registry:   registry,
		strategy:   strategy,
		publisher:  publisher,
	}
}

func (d *Directory) NodeRegistry() *NodeRegistry {
	return d.registry
}

func (d *Directory) Lookup(_ context.Context, key sp.GrainKey) (*sp.Placement, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	placement, ok := d.placements[key]
	if !ok || placement.Status != sp.PlacementStatusActive {
		return nil, sp.ErrPlacementNotFound
	}
	return copyPlacement(placement), nil
}

func (d *Directory) Allocate(ctx context.Context, cmd sp.AllocateCommand) (*sp.Placement, error) {
	key, err := sp.NewGrainKey(cmd.Kind, cmd.GrainID)
	if err != nil {
		return nil, err
	}

	d.mu.RLock()
	if placement, ok := d.placements[key]; ok && placement.Status == sp.PlacementStatusActive {
		d.mu.RUnlock()
		return copyPlacement(placement), nil
	}
	d.mu.RUnlock()

	nodes, err := d.effectiveNodes(ctx, cmd.TargetNodeType, cmd.TargetNodeGroup)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, sp.ErrNoAvailableNode
	}
	chosen, err := d.strategy.Choose(ctx, sp.StrategyInput{
		GrainID:        cmd.GrainID,
		Kind:           cmd.Kind,
		NodeType:       cmd.TargetNodeType,
		NodeGroup:      cmd.TargetNodeGroup,
		EffectiveNodes: nodes,
	})
	if err != nil {
		return nil, err
	}

	now := time.Now()
	ttl := cmd.LeaseTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	placement := sp.Placement{
		GrainID:       cmd.GrainID,
		Kind:          cmd.Kind,
		GrainKey:      key,
		NodeIdentity:  chosen.NodeIdentity,
		Version:       1,
		Status:        sp.PlacementStatusActive,
		CreateTime:    now,
		UpdateTime:    now,
		LeaseExpireAt: now.Add(ttl),
		Lease: sp.Lease{
			OwnerNodeIdentity:  chosen.NodeIdentity,
			OwnerNodeSessionID: chosen.NodeSessionID,
			Version:            1,
			ExpireAt:           now.Add(ttl),
		},
	}

	d.mu.Lock()
	if existing, ok := d.placements[key]; ok && existing.Status == sp.PlacementStatusActive {
		d.mu.Unlock()
		return copyPlacement(existing), nil
	}
	d.placements[key] = placement
	d.addNodeIndexLocked(chosen.NodeIdentity, key)
	d.mu.Unlock()

	_ = d.publish(ctx, placement, sp.EventPlacementCreated)
	return copyPlacement(placement), nil
}

func (d *Directory) Renew(ctx context.Context, cmd sp.RenewCommand) (*sp.Placement, error) {
	d.mu.Lock()
	placement, ok := d.placements[cmd.GrainKey]
	if !ok || placement.Status != sp.PlacementStatusActive {
		d.mu.Unlock()
		return nil, sp.ErrPlacementNotFound
	}
	if placement.NodeIdentity != cmd.NodeIdentity || placement.Lease.OwnerNodeIdentity != cmd.NodeIdentity {
		d.mu.Unlock()
		return nil, sp.ErrInvalidOwner
	}
	if placement.Lease.OwnerNodeSessionID != cmd.NodeSessionID || !d.registry.SessionMatches(cmd.NodeIdentity, cmd.NodeSessionID) {
		d.mu.Unlock()
		return nil, sp.ErrInvalidNodeSession
	}
	if placement.Version != cmd.PlacementVersion || placement.Lease.Version != cmd.LeaseVersion {
		d.mu.Unlock()
		return nil, sp.ErrVersionConflict
	}
	ttl := cmd.ExtendTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	now := time.Now()
	placement.UpdateTime = now
	placement.Lease.Version++
	placement.Lease.ExpireAt = now.Add(ttl)
	placement.LeaseExpireAt = placement.Lease.ExpireAt
	d.placements[cmd.GrainKey] = placement
	d.mu.Unlock()

	_ = d.publish(ctx, placement, sp.EventPlacementRenewed)
	return copyPlacement(placement), nil
}

func (d *Directory) Release(ctx context.Context, cmd sp.ReleaseCommand) error {
	d.mu.Lock()
	placement, ok := d.placements[cmd.GrainKey]
	if !ok || placement.Status != sp.PlacementStatusActive {
		d.mu.Unlock()
		return sp.ErrPlacementNotFound
	}
	if placement.NodeIdentity != cmd.NodeIdentity || placement.Lease.OwnerNodeIdentity != cmd.NodeIdentity {
		d.mu.Unlock()
		return sp.ErrInvalidOwner
	}
	if placement.Lease.OwnerNodeSessionID != cmd.NodeSessionID || !d.registry.SessionMatches(cmd.NodeIdentity, cmd.NodeSessionID) {
		d.mu.Unlock()
		return sp.ErrInvalidNodeSession
	}
	if placement.Version != cmd.PlacementVersion || placement.Lease.Version != cmd.LeaseVersion {
		d.mu.Unlock()
		return sp.ErrVersionConflict
	}
	placement.Status = sp.PlacementStatusReleased
	placement.UpdateTime = time.Now()
	d.placements[cmd.GrainKey] = placement
	d.deleteNodeIndexLocked(placement.NodeIdentity, cmd.GrainKey)
	d.mu.Unlock()

	return d.publish(ctx, placement, sp.EventPlacementReleased)
}

func (d *Directory) Transfer(ctx context.Context, cmd sp.TransferCommand) (*sp.Placement, error) {
	d.mu.Lock()
	placement, ok := d.placements[cmd.GrainKey]
	if !ok || placement.Status != sp.PlacementStatusActive {
		d.mu.Unlock()
		return nil, sp.ErrPlacementNotFound
	}
	if placement.Version != cmd.PlacementVersion {
		d.mu.Unlock()
		return nil, sp.ErrVersionConflict
	}
	if cmd.FromNodeIdentity != "" && placement.NodeIdentity != cmd.FromNodeIdentity {
		d.mu.Unlock()
		return nil, sp.ErrInvalidOwner
	}
	target, ok := d.registry.Node(cmd.ToNodeIdentity)
	if !ok || target.Status != sp.NodeStatusActive || d.registry.IsInvalid(target.NodeType, target.NodeGroup, target.NodeName) {
		d.mu.Unlock()
		return nil, sp.ErrNoAvailableNode
	}
	d.deleteNodeIndexLocked(placement.NodeIdentity, cmd.GrainKey)
	ttl := cmd.LeaseTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	now := time.Now()
	placement.NodeIdentity = target.NodeIdentity
	placement.Version++
	placement.UpdateTime = now
	placement.LeaseExpireAt = now.Add(ttl)
	placement.Lease = sp.Lease{
		OwnerNodeIdentity:  target.NodeIdentity,
		OwnerNodeSessionID: target.NodeSessionID,
		Version:            1,
		ExpireAt:           now.Add(ttl),
	}
	d.placements[cmd.GrainKey] = placement
	d.addNodeIndexLocked(target.NodeIdentity, cmd.GrainKey)
	d.mu.Unlock()

	_ = d.publish(ctx, placement, sp.EventPlacementTransferred)
	return copyPlacement(placement), nil
}

func (d *Directory) Recover(ctx context.Context, cmd sp.RecoverCommand) (*sp.Placement, error) {
	d.mu.Lock()
	placement, ok := d.placements[cmd.GrainKey]
	if !ok {
		d.mu.Unlock()
		return nil, sp.ErrPlacementNotFound
	}
	if placement.Version != cmd.PlacementVersion {
		d.mu.Unlock()
		return nil, sp.ErrVersionConflict
	}
	target, ok := d.registry.Node(cmd.NewNodeIdentity)
	if !ok || target.Status != sp.NodeStatusActive || d.registry.IsInvalid(target.NodeType, target.NodeGroup, target.NodeName) {
		d.mu.Unlock()
		return nil, sp.ErrNoAvailableNode
	}
	d.deleteNodeIndexLocked(placement.NodeIdentity, cmd.GrainKey)
	ttl := cmd.LeaseTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	now := time.Now()
	placement.NodeIdentity = target.NodeIdentity
	placement.Version++
	placement.Status = sp.PlacementStatusActive
	placement.UpdateTime = now
	placement.LeaseExpireAt = now.Add(ttl)
	placement.Lease = sp.Lease{
		OwnerNodeIdentity:  target.NodeIdentity,
		OwnerNodeSessionID: target.NodeSessionID,
		Version:            1,
		ExpireAt:           now.Add(ttl),
	}
	d.placements[cmd.GrainKey] = placement
	d.addNodeIndexLocked(target.NodeIdentity, cmd.GrainKey)
	d.mu.Unlock()

	_ = d.publish(ctx, placement, sp.EventPlacementRecovered)
	return copyPlacement(placement), nil
}

func (d *Directory) Exists(_ context.Context, key sp.GrainKey) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	placement, ok := d.placements[key]
	return ok && placement.Status == sp.PlacementStatusActive, nil
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
		start = parsed
	}
	var keys []string
	for key := range d.byNode[query.NodeIdentity] {
		keys = append(keys, key.String())
	}
	sort.Strings(keys)
	var placements []sp.Placement
	nextIndex := start
	for i := start; i < len(keys) && len(placements) < limit; i++ {
		key := sp.GrainKey(keys[i])
		placement := d.placements[key]
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

func (d *Directory) effectiveNodes(ctx context.Context, nodeType string, nodeGroup string) ([]sp.Node, error) {
	nodes, err := d.registry.FindNodes(ctx, nodeType, nodeGroup)
	if err != nil {
		return nil, err
	}
	effective := nodes[:0]
	for _, node := range nodes {
		if node.Status != sp.NodeStatusActive {
			continue
		}
		if d.registry.IsInvalid(node.NodeType, node.NodeGroup, node.NodeName) {
			continue
		}
		effective = append(effective, node)
	}
	return effective, nil
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
		Type:             eventType,
		GrainKey:         placement.GrainKey,
		NodeIdentity:     placement.NodeIdentity,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
		Time:             time.Now(),
	})
}

func copyPlacement(placement sp.Placement) *sp.Placement {
	copied := placement
	return &copied
}
