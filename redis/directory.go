package redis

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type Directory struct {
	client   goredis.UniversalClient
	strategy sp.PlacementStrategy
}

type mutationArgs struct {
	oldRaw        string
	placement     sp.Placement
	removeOldNode bool
	oldNodeKey    string
	addNewNode    bool
	newNodeKey    string
	leaseMode     string
	eventType     sp.EventType
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

func (d *Directory) RenewNode(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	node, err := d.getNode(ctx, nodeIdentity)
	if err != nil {
		return err
	}
	if node.NodeSessionID != nodeSessionID {
		return sp.ErrInvalidNodeSession
	}
	node.LastHeartbeatAt = time.Now()
	return d.setNode(ctx, *node)
}

func (d *Directory) UnregisterNode(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	node, err := d.getNode(ctx, nodeIdentity)
	if err != nil {
		return err
	}
	if node.NodeSessionID != nodeSessionID {
		return sp.ErrInvalidNodeSession
	}
	if err := d.client.Del(ctx, NodeKey(nodeIdentity)).Err(); err != nil {
		return err
	}
	if err := d.client.SRem(ctx, NodesKey(node.NodeType, node.NodeGroup), nodeIdentity).Err(); err != nil {
		return err
	}
	return d.xadd(ctx, sp.PlacementEvent{
		Type:         sp.EventNodeUnregistered,
		NodeIdentity: nodeIdentity,
		NodeType:     node.NodeType,
		NodeGroup:    node.NodeGroup,
		NodeName:     node.NodeName,
		Time:         time.Now(),
	})
}

func (d *Directory) ReplaceNodeSession(ctx context.Context, node sp.Node) (*sp.Node, error) {
	if node.NodeIdentity == "" {
		identity, err := sp.NewNodeIdentity(node.NodeType, node.NodeGroup, node.NodeName)
		if err != nil {
			return nil, err
		}
		node.NodeIdentity = identity.String()
	}
	if node.Status == "" {
		node.Status = sp.NodeStatusActive
	}
	if node.LastHeartbeatAt.IsZero() {
		node.LastHeartbeatAt = time.Now()
	}
	old, err := d.getNode(ctx, node.NodeIdentity)
	if err != nil && err != sp.ErrNodeNotFound {
		return nil, err
	}
	if err := d.setNode(ctx, node); err != nil {
		return nil, err
	}
	if err := d.client.SAdd(ctx, NodesKey(node.NodeType, node.NodeGroup), node.NodeIdentity).Err(); err != nil {
		return nil, err
	}
	if err := d.xadd(ctx, sp.PlacementEvent{
		Type:         sp.EventNodeReplaced,
		NodeIdentity: node.NodeIdentity,
		NodeType:     node.NodeType,
		NodeGroup:    node.NodeGroup,
		NodeName:     node.NodeName,
		Time:         time.Now(),
	}); err != nil {
		return nil, err
	}
	if old == nil {
		old = &sp.Node{}
	}
	return old, nil
}

func (d *Directory) FindNodes(ctx context.Context, nodeType string, nodeGroup string) ([]sp.Node, error) {
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
		nodes = append(nodes, *node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeIdentity < nodes[j].NodeIdentity
	})
	return nodes, nil
}

func (d *Directory) DrainNode(ctx context.Context, nodeIdentity string) error {
	node, err := d.getNode(ctx, nodeIdentity)
	if err != nil {
		return err
	}
	invalid, err := d.client.SIsMember(ctx, InvalidNodesKey(node.NodeType, node.NodeGroup), node.NodeName).Result()
	if err != nil {
		return err
	}
	if !invalid {
		return sp.ErrNodeNotInvalid
	}
	node.Status = sp.NodeStatusDraining
	if err := d.setNode(ctx, *node); err != nil {
		return err
	}
	return d.xadd(ctx, sp.PlacementEvent{
		Type:         sp.EventNodeDraining,
		NodeIdentity: node.NodeIdentity,
		NodeType:     node.NodeType,
		NodeGroup:    node.NodeGroup,
		NodeName:     node.NodeName,
		Time:         time.Now(),
	})
}

func (d *Directory) CompleteDrain(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	return d.UnregisterNode(ctx, nodeIdentity, nodeSessionID)
}

func (d *Directory) RestoreNode(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error {
	if err := d.client.SRem(ctx, InvalidNodesKey(nodeType, nodeGroup), nodeName).Err(); err != nil {
		return err
	}
	identity, _ := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	return d.xadd(ctx, sp.PlacementEvent{
		Type:         sp.EventNodeRestored,
		NodeIdentity: identity.String(),
		NodeType:     nodeType,
		NodeGroup:    nodeGroup,
		NodeName:     nodeName,
		Time:         time.Now(),
	})
}

func (d *Directory) ListInvalidNodes(ctx context.Context, nodeType string, nodeGroup string) ([]string, error) {
	names, err := d.client.SMembers(ctx, InvalidNodesKey(nodeType, nodeGroup)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
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
	stored, err := d.allocateWithLua(ctx, placement)
	if err != nil {
		return nil, err
	}
	return stored, nil
}

func (d *Directory) Renew(ctx context.Context, cmd sp.RenewCommand) (*sp.Placement, error) {
	oldRaw, placement, err := d.getPlacementRaw(ctx, cmd.GrainKey)
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
	return d.renewWithLua(ctx, string(oldRaw), *placement)
}

func (d *Directory) Release(ctx context.Context, cmd sp.ReleaseCommand) error {
	oldRaw, placement, err := d.getPlacementRaw(ctx, cmd.GrainKey)
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
	_, err = d.mutateWithLua(ctx, mutationArgs{
		oldRaw:        string(oldRaw),
		placement:     *placement,
		removeOldNode: true,
		oldNodeKey:    PlacementNodeKey(placement.NodeIdentity),
		leaseMode:     "remove",
		eventType:     sp.EventPlacementReleased,
	})
	return err
}

func (d *Directory) Transfer(ctx context.Context, cmd sp.TransferCommand) (*sp.Placement, error) {
	oldRaw, placement, err := d.getPlacementRaw(ctx, cmd.GrainKey)
	if err != nil {
		return nil, err
	}
	if placement.Status != sp.PlacementStatusActive {
		return nil, sp.ErrPlacementNotFound
	}
	if placement.Version != cmd.PlacementVersion {
		return nil, sp.ErrVersionConflict
	}
	if cmd.FromNodeIdentity != "" && placement.NodeIdentity != cmd.FromNodeIdentity {
		return nil, sp.ErrInvalidOwner
	}
	target, err := d.effectiveNode(ctx, cmd.ToNodeIdentity)
	if err != nil {
		return nil, err
	}
	oldNodeKey := PlacementNodeKey(placement.NodeIdentity)
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
	return d.mutateWithLua(ctx, mutationArgs{
		oldRaw:        string(oldRaw),
		placement:     *placement,
		removeOldNode: true,
		oldNodeKey:    oldNodeKey,
		addNewNode:    true,
		newNodeKey:    PlacementNodeKey(placement.NodeIdentity),
		leaseMode:     "add",
		eventType:     sp.EventPlacementTransferred,
	})
}

func (d *Directory) Recover(ctx context.Context, cmd sp.RecoverCommand) (*sp.Placement, error) {
	oldRaw, placement, err := d.getPlacementRaw(ctx, cmd.GrainKey)
	if err != nil {
		return nil, err
	}
	if placement.Version != cmd.PlacementVersion {
		return nil, sp.ErrVersionConflict
	}
	target, err := d.effectiveNode(ctx, cmd.NewNodeIdentity)
	if err != nil {
		return nil, err
	}
	oldNodeKey := PlacementNodeKey(placement.NodeIdentity)
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
	return d.mutateWithLua(ctx, mutationArgs{
		oldRaw:        string(oldRaw),
		placement:     *placement,
		removeOldNode: true,
		oldNodeKey:    oldNodeKey,
		addNewNode:    true,
		newNodeKey:    PlacementNodeKey(placement.NodeIdentity),
		leaseMode:     "add",
		eventType:     sp.EventPlacementRecovered,
	})
}

func (d *Directory) Expire(ctx context.Context, cmd sp.ExpireCommand) error {
	now := cmd.Now
	if now.IsZero() {
		now = time.Now()
	}
	oldRaw, placement, err := d.getPlacementRaw(ctx, cmd.GrainKey)
	if err != nil {
		return err
	}
	if placement.Status != sp.PlacementStatusActive {
		return sp.ErrPlacementNotFound
	}
	if placement.Lease.Version != cmd.LeaseVersion {
		return sp.ErrVersionConflict
	}
	if now.Before(placement.LeaseExpireAt) {
		return sp.ErrLeaseNotExpired
	}
	placement.Status = sp.PlacementStatusExpired
	placement.UpdateTime = now
	_, err = d.mutateWithLua(ctx, mutationArgs{
		oldRaw:        string(oldRaw),
		placement:     *placement,
		removeOldNode: true,
		oldNodeKey:    PlacementNodeKey(placement.NodeIdentity),
		leaseMode:     "remove",
		eventType:     sp.EventLeaseExpired,
	})
	return err
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
	min := "-inf"
	if query.Cursor != "" {
		score, err := parseCursorScore(query.Cursor)
		if err != nil {
			return sp.PlacementPage{}, err
		}
		min = "(" + strconv.FormatInt(score, 10)
	}
	values, err := d.client.ZRangeByScoreWithScores(ctx, PlacementNodeKey(query.NodeIdentity), &goredis.ZRangeBy{
		Min: min,
		Max: "+inf",
	}).Result()
	if err != nil {
		return sp.PlacementPage{}, err
	}
	status := query.Status
	if status == "" {
		status = sp.PlacementStatusActive
	}
	var placements []sp.Placement
	var lastScore int64
	var lastKey string
	for _, value := range values {
		key, ok := value.Member.(string)
		if !ok {
			continue
		}
		placement, err := d.getPlacement(ctx, sp.GrainKey(key))
		if err != nil {
			continue
		}
		if placement.Status == status {
			placements = append(placements, *placement)
			lastScore = int64(value.Score)
			lastKey = key
			if len(placements) == limit {
				break
			}
		}
	}
	next := ""
	if len(placements) == limit && hasPlacementAfterScore(values, lastScore) {
		next = formatCursor(lastScore, lastKey)
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

func (d *Directory) effectiveNode(ctx context.Context, nodeIdentity string) (*sp.Node, error) {
	node, err := d.getNode(ctx, nodeIdentity)
	if err != nil {
		return nil, err
	}
	if node.Status != sp.NodeStatusActive {
		return nil, sp.ErrNoAvailableNode
	}
	invalid, err := d.client.SIsMember(ctx, InvalidNodesKey(node.NodeType, node.NodeGroup), node.NodeName).Result()
	if err != nil {
		return nil, err
	}
	if invalid {
		return nil, sp.ErrNoAvailableNode
	}
	return node, nil
}

func (d *Directory) getPlacement(ctx context.Context, key sp.GrainKey) (*sp.Placement, error) {
	_, placement, err := d.getPlacementRaw(ctx, key)
	return placement, err
}

func (d *Directory) getPlacementRaw(ctx context.Context, key sp.GrainKey) ([]byte, *sp.Placement, error) {
	value, err := d.client.Get(ctx, PlacementKey(key)).Bytes()
	if err == goredis.Nil {
		return nil, nil, sp.ErrPlacementNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	var placement sp.Placement
	if err := json.Unmarshal(value, &placement); err != nil {
		return nil, nil, err
	}
	return value, &placement, nil
}

func (d *Directory) setPlacement(ctx context.Context, placement sp.Placement) error {
	value, err := json.Marshal(placement)
	if err != nil {
		return err
	}
	return d.client.Set(ctx, PlacementKey(placement.GrainKey), value, 0).Err()
}

func (d *Directory) addPlacementNodeIndex(ctx context.Context, nodeIdentity string, key sp.GrainKey) error {
	score, err := d.client.Incr(ctx, SequenceKey()).Result()
	if err != nil {
		return err
	}
	return d.client.ZAdd(ctx, PlacementNodeKey(nodeIdentity), goredis.Z{Score: float64(score), Member: key.String()}).Err()
}

func (d *Directory) allocateWithLua(ctx context.Context, placement sp.Placement) (*sp.Placement, error) {
	value, err := json.Marshal(placement)
	if err != nil {
		return nil, err
	}
	result, err := d.client.Eval(ctx, allocateLua, []string{
		PlacementKey(placement.GrainKey),
		PlacementNodeKey(placement.NodeIdentity),
		LeaseExpireKey(),
		SequenceKey(),
		EventsStreamKey(),
	},
		string(value),
		placement.GrainKey.String(),
		strconv.FormatInt(placement.LeaseExpireAt.UnixMilli(), 10),
		string(sp.EventPlacementCreated),
		placement.NodeIdentity,
		strconv.FormatInt(placement.Version, 10),
		strconv.FormatInt(placement.Lease.Version, 10),
	).Text()
	if err != nil {
		return nil, err
	}
	var stored sp.Placement
	if err := json.Unmarshal([]byte(result), &stored); err != nil {
		return nil, err
	}
	return &stored, nil
}

func (d *Directory) renewWithLua(ctx context.Context, oldRaw string, placement sp.Placement) (*sp.Placement, error) {
	value, err := json.Marshal(placement)
	if err != nil {
		return nil, err
	}
	result, err := d.client.Eval(ctx, renewLua, []string{
		PlacementKey(placement.GrainKey),
		LeaseExpireKey(),
		AuditStreamKey(),
	},
		oldRaw,
		string(value),
		strconv.FormatInt(placement.LeaseExpireAt.UnixMilli(), 10),
		placement.GrainKey.String(),
		string(sp.EventPlacementRenewed),
		placement.NodeIdentity,
		strconv.FormatInt(placement.Version, 10),
		strconv.FormatInt(placement.Lease.Version, 10),
	).Text()
	if err != nil {
		return nil, err
	}
	if result == "conflict" {
		return nil, sp.ErrVersionConflict
	}
	var stored sp.Placement
	if err := json.Unmarshal([]byte(result), &stored); err != nil {
		return nil, err
	}
	return &stored, nil
}

func (d *Directory) mutateWithLua(ctx context.Context, args mutationArgs) (*sp.Placement, error) {
	value, err := json.Marshal(args.placement)
	if err != nil {
		return nil, err
	}
	oldNodeKey := args.oldNodeKey
	if oldNodeKey == "" {
		oldNodeKey = PlacementNodeKey(args.placement.NodeIdentity)
	}
	newNodeKey := args.newNodeKey
	if newNodeKey == "" {
		newNodeKey = PlacementNodeKey(args.placement.NodeIdentity)
	}
	removeOldNode := "0"
	if args.removeOldNode {
		removeOldNode = "1"
	}
	addNewNode := "0"
	if args.addNewNode {
		addNewNode = "1"
	}
	leaseMode := args.leaseMode
	if leaseMode == "" {
		leaseMode = "keep"
	}
	result, err := d.client.Eval(ctx, mutationLua, []string{
		PlacementKey(args.placement.GrainKey),
		oldNodeKey,
		newNodeKey,
		LeaseExpireKey(),
		SequenceKey(),
		EventsStreamKey(),
	},
		args.oldRaw,
		string(value),
		removeOldNode,
		addNewNode,
		leaseMode,
		args.placement.GrainKey.String(),
		strconv.FormatInt(args.placement.LeaseExpireAt.UnixMilli(), 10),
		string(args.eventType),
		args.placement.NodeIdentity,
		strconv.FormatInt(args.placement.Version, 10),
		strconv.FormatInt(args.placement.Lease.Version, 10),
	).Text()
	if err != nil {
		return nil, err
	}
	if result == "conflict" {
		return nil, sp.ErrVersionConflict
	}
	var stored sp.Placement
	if err := json.Unmarshal([]byte(result), &stored); err != nil {
		return nil, err
	}
	return &stored, nil
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

func (d *Directory) setNode(ctx context.Context, node sp.Node) error {
	value, err := json.Marshal(node)
	if err != nil {
		return err
	}
	return d.client.Set(ctx, NodeKey(node.NodeIdentity), value, 0).Err()
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

func parseCursorScore(cursor string) (int64, error) {
	score, _, _ := strings.Cut(cursor, ":")
	return strconv.ParseInt(score, 10, 64)
}

func formatCursor(score int64, key string) string {
	return strconv.FormatInt(score, 10) + ":" + key
}

func hasPlacementAfterScore(values []goredis.Z, score int64) bool {
	for _, value := range values {
		if int64(value.Score) > score {
			return true
		}
	}
	return false
}
