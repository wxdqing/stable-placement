package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type Directory struct {
	client   goredis.UniversalClient
	strategy sp.PlacementStrategy
}

func NewDirectory(client goredis.UniversalClient, strategy sp.PlacementStrategy) *Directory {
	return &Directory{client: client, strategy: strategy}
}

func (d *Directory) RegisterNode(ctx context.Context, node sp.Node) error {
	if node.NodeIdentity == "" {
		identity, err := sp.NewNodeIdentity(node.NodeType, node.NodeGroup, node.NodeName)
		if err != nil {
			return err
		}
		node.NodeIdentity = identity.String()
	}
	if node.Status == "" {
		node.Status = sp.NodeStatusActive
	}
	if node.LastHeartbeatAt.IsZero() {
		node.LastHeartbeatAt = time.Now()
	}
	data, err := json.Marshal(node)
	if err != nil {
		return err
	}
	if err := d.client.Set(ctx, NodeKey(node.NodeIdentity), data, 0).Err(); err != nil {
		return err
	}
	if err := d.client.SAdd(ctx, NodesKey(node.NodeType, node.NodeGroup), node.NodeIdentity).Err(); err != nil {
		return err
	}
	return d.xadd(ctx, sp.PlacementEvent{
		Type:         sp.EventNodeRegistered,
		NodeIdentity: node.NodeIdentity,
		NodeType:     node.NodeType,
		NodeGroup:    node.NodeGroup,
		NodeName:     node.NodeName,
		Time:         time.Now(),
	})
}

func (d *Directory) MarkNodeInvalid(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error {
	if err := d.client.SAdd(ctx, InvalidNodesKey(nodeType, nodeGroup), nodeName).Err(); err != nil {
		return err
	}
	identity, _ := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	return d.xadd(ctx, sp.PlacementEvent{
		Type:         sp.EventNodeMarkedInvalid,
		NodeIdentity: identity.String(),
		NodeType:     nodeType,
		NodeGroup:    nodeGroup,
		NodeName:     nodeName,
		Time:         time.Now(),
	})
}

func (d *Directory) Lookup(ctx context.Context, key sp.GrainKey) (*sp.Placement, error) {
	placement, err := d.getPlacement(ctx, key)
	if err != nil {
		return nil, err
	}
	if placement.Status != sp.PlacementStatusActive {
		return nil, sp.ErrPlacementNotFound
	}
	return placement, nil
}

func (d *Directory) Allocate(ctx context.Context, cmd sp.AllocateCommand) (*sp.Placement, error) {
	key, err := sp.NewGrainKey(cmd.Kind, cmd.GrainID)
	if err != nil {
		return nil, err
	}
	if existing, err := d.Lookup(ctx, key); err == nil {
		return existing, nil
	}

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
	if err := d.setPlacement(ctx, placement); err != nil {
		return nil, err
	}
	score, err := d.client.Incr(ctx, SequenceKey()).Result()
	if err != nil {
		return nil, err
	}
	if err := d.client.ZAdd(ctx, PlacementNodeKey(chosen.NodeIdentity), goredis.Z{Score: float64(score), Member: key.String()}).Err(); err != nil {
		return nil, err
	}
	if err := d.client.ZAdd(ctx, LeaseExpireKey(), goredis.Z{Score: float64(placement.LeaseExpireAt.UnixMilli()), Member: key.String()}).Err(); err != nil {
		return nil, err
	}
	if err := d.xadd(ctx, eventFromPlacement(sp.EventPlacementCreated, placement)); err != nil {
		return nil, err
	}
	return &placement, nil
}

func (d *Directory) Renew(ctx context.Context, cmd sp.RenewCommand) (*sp.Placement, error) {
	placement, err := d.getPlacement(ctx, cmd.GrainKey)
	if err != nil {
		return nil, err
	}
	if placement.Status != sp.PlacementStatusActive {
		return nil, sp.ErrPlacementNotFound
	}
	if placement.NodeIdentity != cmd.NodeIdentity || placement.Lease.OwnerNodeIdentity != cmd.NodeIdentity {
		return nil, sp.ErrInvalidOwner
	}
	node, err := d.getNode(ctx, cmd.NodeIdentity)
	if err != nil {
		return nil, err
	}
	if placement.Lease.OwnerNodeSessionID != cmd.NodeSessionID || node.NodeSessionID != cmd.NodeSessionID {
		return nil, sp.ErrInvalidNodeSession
	}
	if placement.Version != cmd.PlacementVersion || placement.Lease.Version != cmd.LeaseVersion {
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
	if err := d.setPlacement(ctx, *placement); err != nil {
		return nil, err
	}
	if err := d.client.ZAdd(ctx, LeaseExpireKey(), goredis.Z{Score: float64(placement.LeaseExpireAt.UnixMilli()), Member: cmd.GrainKey.String()}).Err(); err != nil {
		return nil, err
	}
	return placement, nil
}

func (d *Directory) Release(ctx context.Context, cmd sp.ReleaseCommand) error {
	placement, err := d.getPlacement(ctx, cmd.GrainKey)
	if err != nil {
		return err
	}
	if placement.Status != sp.PlacementStatusActive {
		return sp.ErrPlacementNotFound
	}
	if placement.NodeIdentity != cmd.NodeIdentity || placement.Lease.OwnerNodeIdentity != cmd.NodeIdentity {
		return sp.ErrInvalidOwner
	}
	node, err := d.getNode(ctx, cmd.NodeIdentity)
	if err != nil {
		return err
	}
	if placement.Lease.OwnerNodeSessionID != cmd.NodeSessionID || node.NodeSessionID != cmd.NodeSessionID {
		return sp.ErrInvalidNodeSession
	}
	if placement.Version != cmd.PlacementVersion || placement.Lease.Version != cmd.LeaseVersion {
		return sp.ErrVersionConflict
	}
	placement.Status = sp.PlacementStatusReleased
	placement.UpdateTime = time.Now()
	if err := d.setPlacement(ctx, *placement); err != nil {
		return err
	}
	if err := d.client.ZRem(ctx, PlacementNodeKey(placement.NodeIdentity), cmd.GrainKey.String()).Err(); err != nil {
		return err
	}
	if err := d.client.ZRem(ctx, LeaseExpireKey(), cmd.GrainKey.String()).Err(); err != nil {
		return err
	}
	return d.xadd(ctx, eventFromPlacement(sp.EventPlacementReleased, *placement))
}

func (d *Directory) Transfer(ctx context.Context, cmd sp.TransferCommand) (*sp.Placement, error) {
	return nil, sp.ErrPlacementNotFound
}

func (d *Directory) Recover(ctx context.Context, cmd sp.RecoverCommand) (*sp.Placement, error) {
	return nil, sp.ErrPlacementNotFound
}

func (d *Directory) Expire(ctx context.Context, cmd sp.ExpireCommand) error {
	return sp.ErrPlacementNotFound
}

func (d *Directory) Exists(ctx context.Context, key sp.GrainKey) (bool, error) {
	placement, err := d.Lookup(ctx, key)
	return err == nil && placement != nil, nil
}

func (d *Directory) FindByNode(ctx context.Context, query sp.FindByNodeQuery) (sp.PlacementPage, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	start := int64(0)
	if query.Cursor != "" {
		parsed, err := strconv.ParseInt(query.Cursor, 10, 64)
		if err != nil {
			return sp.PlacementPage{}, err
		}
		start = parsed
	}
	values, err := d.client.ZRange(ctx, PlacementNodeKey(query.NodeIdentity), start, start+int64(limit)-1).Result()
	if err != nil {
		return sp.PlacementPage{}, err
	}
	status := query.Status
	if status == "" {
		status = sp.PlacementStatusActive
	}
	var placements []sp.Placement
	for _, value := range values {
		placement, err := d.getPlacement(ctx, sp.GrainKey(value))
		if err != nil {
			continue
		}
		if placement.Status == status {
			placements = append(placements, *placement)
		}
	}
	next := ""
	if len(values) == limit {
		next = strconv.FormatInt(start+int64(limit), 10)
	}
	return sp.PlacementPage{Placements: placements, NextCursor: next}, nil
}

func (d *Directory) effectiveNodes(ctx context.Context, nodeType string, nodeGroup string) ([]sp.Node, error) {
	identities, err := d.client.SMembers(ctx, NodesKey(nodeType, nodeGroup)).Result()
	if err != nil {
		return nil, err
	}
	var nodes []sp.Node
	for _, identity := range identities {
		node, err := d.getNode(ctx, identity)
		if err != nil {
			continue
		}
		if node.Status != sp.NodeStatusActive {
			continue
		}
		invalid, err := d.client.SIsMember(ctx, InvalidNodesKey(node.NodeType, node.NodeGroup), node.NodeName).Result()
		if err != nil {
			return nil, err
		}
		if invalid {
			continue
		}
		nodes = append(nodes, *node)
	}
	return nodes, nil
}

func (d *Directory) getPlacement(ctx context.Context, key sp.GrainKey) (*sp.Placement, error) {
	value, err := d.client.Get(ctx, PlacementKey(key)).Bytes()
	if err == goredis.Nil {
		return nil, sp.ErrPlacementNotFound
	}
	if err != nil {
		return nil, err
	}
	var placement sp.Placement
	if err := json.Unmarshal(value, &placement); err != nil {
		return nil, err
	}
	return &placement, nil
}

func (d *Directory) setPlacement(ctx context.Context, placement sp.Placement) error {
	value, err := json.Marshal(placement)
	if err != nil {
		return err
	}
	return d.client.Set(ctx, PlacementKey(placement.GrainKey), value, 0).Err()
}

func (d *Directory) getNode(ctx context.Context, nodeIdentity string) (*sp.Node, error) {
	value, err := d.client.Get(ctx, NodeKey(nodeIdentity)).Bytes()
	if err == goredis.Nil {
		return nil, sp.ErrNodeNotFound
	}
	if err != nil {
		return nil, err
	}
	var node sp.Node
	if err := json.Unmarshal(value, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

func (d *Directory) xadd(ctx context.Context, event sp.PlacementEvent) error {
	return d.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: EventsStreamKey(),
		Values: map[string]any{
			"type":              string(event.Type),
			"grain_key":         event.GrainKey.String(),
			"node_identity":     event.NodeIdentity,
			"node_type":         event.NodeType,
			"node_group":        event.NodeGroup,
			"node_name":         event.NodeName,
			"placement_version": event.PlacementVersion,
			"lease_version":     event.LeaseVersion,
		},
	}).Err()
}

func eventFromPlacement(eventType sp.EventType, placement sp.Placement) sp.PlacementEvent {
	return sp.PlacementEvent{
		Type:             eventType,
		GrainKey:         placement.GrainKey,
		NodeIdentity:     placement.NodeIdentity,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
		Time:             time.Now(),
	}
}
