package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type Directory struct {
	client goredis.UniversalClient
	mode   sp.StrategyMode

	heartbeatMu  sync.RWMutex
	heartbeatTTL time.Duration
}

type mutationArgs struct {
	oldRaw           string
	placement        sp.Placement
	removeOldNode    bool
	oldNodeKey       string
	addNewNode       bool
	newNodeKey       string
	leaseMode        string
	eventType        sp.EventType
	checkNodeSession bool
	nodeKey          string
	nodeSessionID    string
	checkTargetNode  bool
	targetNodeKey    string
	invalidNodesKey  string
	targetNodeType   string
	targetNodeGroup  string
	targetNodeName   string
}

type redisNode struct {
	sp.Node
	PlacementNodeKey string
}

const maxPlacementIndexScore = int64(1<<53 - 1)

func NewDirectory(client goredis.UniversalClient, mode sp.StrategyMode) (*Directory, error) {
	if mode != sp.StrategyModeRedisRoundRobin {
		return nil, sp.ErrUnsupportedStrategyMode
	}
	return &Directory{client: client, mode: mode, heartbeatTTL: time.Minute}, nil
}

func (d *Directory) RegisterNode(ctx context.Context, node sp.Node) error {
	if err := normalizeNode(&node); err != nil {
		return err
	}
	return d.registerNodeWithLua(ctx, node, sp.EventNodeRegistered)
}

func (d *Directory) MarkNodeInvalid(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error {
	identity, _ := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	return d.markInvalidWithLua(ctx, nodeType, nodeGroup, nodeName, identity.String())
}

func (d *Directory) RenewNode(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	node, err := d.getNode(ctx, nodeIdentity)
	if err != nil {
		return err
	}
	now := time.Now()
	result, err := d.client.Eval(ctx, renewNodeLua, []string{
		NodeKey(nodeIdentity),
		NodeHeartbeatKey(node.NodeType, node.NodeGroup),
	},
		nodeSessionID,
		now.Format(time.RFC3339Nano),
		strconv.FormatInt(now.UnixMilli(), 10),
		NodeKey(nodeIdentity),
	).Text()
	if err != nil {
		return err
	}
	switch result {
	case "node_not_found":
		return sp.ErrNodeNotFound
	case "invalid_node_session":
		return sp.ErrInvalidNodeSession
	default:
		return nil
	}
}

func (d *Directory) UnregisterNode(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	node, err := d.getNode(ctx, nodeIdentity)
	if err != nil {
		return err
	}
	return d.unregisterNodeWithLua(ctx, *node, nodeSessionID, false)
}

func (d *Directory) ReplaceNodeSession(ctx context.Context, node sp.Node) (*sp.Node, error) {
	if err := normalizeNode(&node); err != nil {
		return nil, err
	}
	old, err := d.replaceNodeSessionWithLua(ctx, node)
	if err != nil {
		return nil, err
	}
	return old, nil
}

func (d *Directory) FindNodes(ctx context.Context, nodeType string, nodeGroup string) ([]sp.Node, error) {
	members, err := d.client.SMembers(ctx, NodesKey(nodeType, nodeGroup)).Result()
	if err != nil {
		return nil, err
	}
	var nodes []sp.Node
	for _, member := range members {
		node, err := d.getNodeFromMember(ctx, member)
		if errors.Is(err, sp.ErrNodeNotFound) {
			continue
		}
		if err != nil {
			return nil, err
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
	return d.drainNodeWithLua(ctx, *node)
}

func (d *Directory) CompleteDrain(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	node, err := d.getNode(ctx, nodeIdentity)
	if err != nil {
		return err
	}
	return d.unregisterNodeWithLua(ctx, *node, nodeSessionID, true)
}

func (d *Directory) RestoreNode(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error {
	identity, _ := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	return d.restoreNodeWithLua(ctx, nodeType, nodeGroup, nodeName, identity.String())
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
	if !placement.LeaseExpireAt.IsZero() && !time.Now().Before(placement.LeaseExpireAt) {
		return nil, sp.ErrPlacementNotFound
	}
	return placement, nil
}

func (d *Directory) Allocate(ctx context.Context, cmd sp.AllocateCommand) (*sp.Placement, error) {
	key, err := sp.NewGrainKey(cmd.Kind, cmd.GrainID)
	if err != nil {
		return nil, err
	}
	ttl := cmd.LeaseTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	for attempt := 0; attempt < 2; attempt++ {
		oldRaw, existing, err := d.getPlacementRaw(ctx, key)
		if err != nil && !errors.Is(err, sp.ErrPlacementNotFound) {
			return nil, err
		}
		oldNodeKey := ""
		if err == nil {
			oldNodeKey = PlacementNodeKey(existing.NodeIdentity)
			if existing.Status == sp.PlacementStatusActive &&
				(existing.LeaseExpireAt.IsZero() || time.Now().Before(existing.LeaseExpireAt)) {
				return existing, nil
			}
		}

		now := time.Now()
		placement := sp.Placement{
			GrainID:       cmd.GrainID,
			Kind:          cmd.Kind,
			GrainKey:      key,
			Status:        sp.PlacementStatusActive,
			CreateTime:    now,
			UpdateTime:    now,
			LeaseExpireAt: now.Add(ttl),
			Lease: sp.Lease{
				Version:  1,
				ExpireAt: now.Add(ttl),
			},
		}
		stored, err := d.allocateWithLua(ctx, placement, cmd.TargetNodeType, cmd.TargetNodeGroup, string(oldRaw), oldNodeKey, now)
		if !errors.Is(err, sp.ErrVersionConflict) || attempt == 1 {
			return stored, err
		}
	}
	return nil, sp.ErrVersionConflict
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
	placement.Version++
	placement.Status = sp.PlacementStatusReleased
	placement.UpdateTime = time.Now()
	_, err = d.mutateWithLua(ctx, mutationArgs{
		oldRaw:           string(oldRaw),
		placement:        *placement,
		removeOldNode:    true,
		oldNodeKey:       PlacementNodeKey(placement.NodeIdentity),
		leaseMode:        "remove",
		eventType:        sp.EventPlacementReleased,
		checkNodeSession: true,
		nodeKey:          NodeKey(cmd.NodeIdentity),
		nodeSessionID:    cmd.NodeSessionID,
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
		oldRaw:          string(oldRaw),
		placement:       *placement,
		removeOldNode:   true,
		oldNodeKey:      oldNodeKey,
		addNewNode:      true,
		newNodeKey:      PlacementNodeKey(placement.NodeIdentity),
		leaseMode:       "add",
		eventType:       sp.EventPlacementTransferred,
		checkTargetNode: true,
		targetNodeKey:   NodeKey(target.NodeIdentity),
		invalidNodesKey: InvalidNodesKey(target.NodeType, target.NodeGroup),
		targetNodeType:  target.NodeType,
		targetNodeGroup: target.NodeGroup,
		targetNodeName:  target.NodeName,
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
	if !sp.PlacementRecoverable(placement.Status) {
		return nil, sp.ErrPlacementNotRecoverable
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
		oldRaw:          string(oldRaw),
		placement:       *placement,
		removeOldNode:   true,
		oldNodeKey:      oldNodeKey,
		addNewNode:      true,
		newNodeKey:      PlacementNodeKey(placement.NodeIdentity),
		leaseMode:       "add",
		eventType:       sp.EventPlacementRecovered,
		checkTargetNode: true,
		targetNodeKey:   NodeKey(target.NodeIdentity),
		invalidNodesKey: InvalidNodesKey(target.NodeType, target.NodeGroup),
		targetNodeType:  target.NodeType,
		targetNodeGroup: target.NodeGroup,
		targetNodeName:  target.NodeName,
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
	placement.Version++
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
	if errors.Is(err, sp.ErrPlacementNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return placement != nil, nil
}

func (d *Directory) FindByNode(ctx context.Context, query sp.FindByNodeQuery) (sp.PlacementPage, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	batchSize := int64(max(limit, 100))
	min := "-inf"
	var boundaryScore int64
	var boundaryKey string
	hasBoundary := false
	if query.Cursor != "" {
		score, key, err := parseCursor(query.Cursor)
		if err != nil {
			return sp.PlacementPage{}, err
		}
		if score < 0 || score > maxPlacementIndexScore {
			return sp.PlacementPage{}, fmt.Errorf("invalid placement index score %d in cursor", score)
		}
		boundaryScore = score
		boundaryKey = key
		hasBoundary = true
		min = strconv.FormatInt(score, 10)
	}
	status := query.Status
	if status == "" {
		status = sp.PlacementStatusActive
	}
	var placements []sp.Placement
	var placementScores []int64
	var placementKeys []string
	previousScore := boundaryScore
	hasPreviousScore := hasBoundary
	indexKey := PlacementNodeKey(query.NodeIdentity)
	for {
		values, err := d.client.ZRangeByScoreWithScores(ctx, indexKey, &goredis.ZRangeBy{
			Min:   min,
			Max:   "+inf",
			Count: batchSize,
		}).Result()
		if err != nil {
			return sp.PlacementPage{}, err
		}
		if len(values) == 0 {
			break
		}
		var lastScore int64
		var lastKey string
		for index, value := range values {
			if math.IsNaN(value.Score) || math.IsInf(value.Score, 0) || math.Trunc(value.Score) != value.Score || value.Score < 0 || value.Score > float64(maxPlacementIndexScore) {
				return sp.PlacementPage{}, fmt.Errorf("invalid placement index score %v", value.Score)
			}
			score := int64(value.Score)
			key, ok := value.Member.(string)
			if !ok {
				return sp.PlacementPage{}, fmt.Errorf("invalid placement index member type %T", value.Member)
			}
			lastScore = score
			lastKey = key
			if hasBoundary && index == 0 {
				if score < boundaryScore {
					return sp.PlacementPage{}, fmt.Errorf("invalid placement index score %d before cursor score %d", score, boundaryScore)
				}
				if score == boundaryScore {
					if key != boundaryKey {
						return sp.PlacementPage{}, fmt.Errorf("invalid placement index score %d: duplicate member %q after %q", score, key, boundaryKey)
					}
					continue
				}
			}
			if hasPreviousScore && score <= previousScore {
				return sp.PlacementPage{}, fmt.Errorf("invalid placement index score %d: scores must be unique and increasing", score)
			}
			previousScore = score
			hasPreviousScore = true
			placement, err := d.getPlacement(ctx, sp.GrainKey(key))
			if errors.Is(err, sp.ErrPlacementNotFound) {
				continue
			}
			if err != nil {
				return sp.PlacementPage{}, err
			}
			if placement.Status != status {
				continue
			}
			placements = append(placements, *placement)
			placementScores = append(placementScores, score)
			placementKeys = append(placementKeys, key)
			if len(placements) > limit {
				return sp.PlacementPage{
					Placements: placements[:limit],
					NextCursor: formatCursor(placementScores[limit-1], placementKeys[limit-1]),
				}, nil
			}
		}
		if int64(len(values)) < batchSize {
			break
		}
		boundaryScore = lastScore
		boundaryKey = lastKey
		hasBoundary = true
		min = strconv.FormatInt(lastScore, 10)
	}
	return sp.PlacementPage{Placements: placements}, nil
}

func (d *Directory) effectiveNodes(ctx context.Context, nodeType string, nodeGroup string) ([]sp.Node, error) {
	members, err := d.client.SMembers(ctx, NodesKey(nodeType, nodeGroup)).Result()
	if err != nil {
		return nil, err
	}
	var nodes []sp.Node
	for _, member := range members {
		node, err := d.getNodeFromMember(ctx, member)
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

func (d *Directory) allocateWithLua(ctx context.Context, placement sp.Placement, nodeType string, nodeGroup string, oldRaw string, oldNodeKey string, now time.Time) (*sp.Placement, error) {
	if oldNodeKey == "" {
		oldNodeKey = PlacementNodeKey("")
	}
	result, err := d.client.Eval(ctx, allocateLua, []string{
		PlacementKey(placement.GrainKey),
		NodesKey(nodeType, nodeGroup),
		InvalidNodesKey(nodeType, nodeGroup),
		StrategyRoundRobinKey(nodeType, nodeGroup),
		LeaseExpireKey(),
		SequenceKey(),
		EventsStreamKey(),
		oldNodeKey,
	},
		placement.GrainID,
		placement.Kind,
		placement.GrainKey.String(),
		placement.CreateTime.Format(time.RFC3339Nano),
		placement.LeaseExpireAt.Format(time.RFC3339Nano),
		strconv.FormatInt(placement.LeaseExpireAt.UnixMilli(), 10),
		string(sp.EventPlacementCreated),
		strconv.FormatInt(now.UnixMilli(), 10),
		oldRaw,
	).Text()
	if err != nil {
		return nil, err
	}
	if result == "no_available_node" {
		return nil, sp.ErrNoAvailableNode
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

func (d *Directory) renewWithLua(ctx context.Context, oldRaw string, placement sp.Placement) (*sp.Placement, error) {
	value, err := json.Marshal(placement)
	if err != nil {
		return nil, err
	}
	result, err := d.client.Eval(ctx, renewLua, []string{
		PlacementKey(placement.GrainKey),
		LeaseExpireKey(),
		AuditStreamKey(),
		NodeKey(placement.NodeIdentity),
	},
		oldRaw,
		string(value),
		strconv.FormatInt(placement.LeaseExpireAt.UnixMilli(), 10),
		placement.GrainKey.String(),
		string(sp.EventPlacementRenewed),
		placement.NodeIdentity,
		strconv.FormatInt(placement.Version, 10),
		strconv.FormatInt(placement.Lease.Version, 10),
		placement.Lease.OwnerNodeSessionID,
	).Text()
	if err != nil {
		return nil, err
	}
	if result == "invalid_node_session" {
		return nil, sp.ErrInvalidNodeSession
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
	checkNodeSession := "0"
	if args.checkNodeSession {
		checkNodeSession = "1"
	}
	nodeKey := args.nodeKey
	if nodeKey == "" {
		nodeKey = NodeKey(args.placement.NodeIdentity)
	}
	checkTargetNode := "0"
	if args.checkTargetNode {
		checkTargetNode = "1"
	}
	targetNodeKey := args.targetNodeKey
	if targetNodeKey == "" {
		targetNodeKey = nodeKey
	}
	invalidNodesKey := args.invalidNodesKey
	if invalidNodesKey == "" {
		invalidNodesKey = targetNodeKey
	}
	result, err := d.client.Eval(ctx, mutationLua, []string{
		PlacementKey(args.placement.GrainKey),
		oldNodeKey,
		newNodeKey,
		LeaseExpireKey(),
		SequenceKey(),
		EventsStreamKey(),
		nodeKey,
		targetNodeKey,
		invalidNodesKey,
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
		checkNodeSession,
		args.nodeSessionID,
		checkTargetNode,
		args.targetNodeType,
		args.targetNodeGroup,
		args.targetNodeName,
	).Text()
	if err != nil {
		return nil, err
	}
	if result == "invalid_node_session" {
		return nil, sp.ErrInvalidNodeSession
	}
	if result == "conflict" {
		return nil, sp.ErrVersionConflict
	}
	if result == "no_available_node" {
		return nil, sp.ErrNoAvailableNode
	}
	var stored sp.Placement
	if err := json.Unmarshal([]byte(result), &stored); err != nil {
		return nil, err
	}
	return &stored, nil
}

func (d *Directory) registerNodeWithLua(ctx context.Context, node sp.Node, eventType sp.EventType) error {
	value, err := marshalRedisNode(node)
	if err != nil {
		return err
	}
	return d.client.Eval(ctx, registerNodeLua, []string{
		NodeKey(node.NodeIdentity),
		NodesKey(node.NodeType, node.NodeGroup),
		EventsStreamKey(),
		NodeHeartbeatKey(node.NodeType, node.NodeGroup),
	},
		string(value),
		node.NodeIdentity,
		string(eventType),
		node.NodeType,
		node.NodeGroup,
		node.NodeName,
		NodeKey(node.NodeIdentity),
		strconv.FormatInt(node.LastHeartbeatAt.UnixMilli(), 10),
	).Err()
}

func (d *Directory) replaceNodeSessionWithLua(ctx context.Context, node sp.Node) (*sp.Node, error) {
	value, err := marshalRedisNode(node)
	if err != nil {
		return nil, err
	}
	result, err := d.client.Eval(ctx, replaceNodeSessionLua, []string{
		NodeKey(node.NodeIdentity),
		NodesKey(node.NodeType, node.NodeGroup),
		EventsStreamKey(),
		NodeHeartbeatKey(node.NodeType, node.NodeGroup),
	},
		string(value),
		node.NodeIdentity,
		string(sp.EventNodeReplaced),
		node.NodeType,
		node.NodeGroup,
		node.NodeName,
		NodeKey(node.NodeIdentity),
		strconv.FormatInt(node.LastHeartbeatAt.UnixMilli(), 10),
	).Text()
	if err != nil {
		return nil, err
	}
	if result == "" {
		return &sp.Node{}, nil
	}
	var old sp.Node
	if err := json.Unmarshal([]byte(result), &old); err != nil {
		return nil, err
	}
	return &old, nil
}

func (d *Directory) markInvalidWithLua(ctx context.Context, nodeType string, nodeGroup string, nodeName string, nodeIdentity string) error {
	return d.client.Eval(ctx, markNodeInvalidLua, []string{
		InvalidNodesKey(nodeType, nodeGroup),
		EventsStreamKey(),
	},
		nodeName,
		string(sp.EventNodeMarkedInvalid),
		nodeIdentity,
		nodeType,
		nodeGroup,
	).Err()
}

func (d *Directory) restoreNodeWithLua(ctx context.Context, nodeType string, nodeGroup string, nodeName string, nodeIdentity string) error {
	return d.client.Eval(ctx, restoreNodeLua, []string{
		InvalidNodesKey(nodeType, nodeGroup),
		EventsStreamKey(),
	},
		nodeName,
		string(sp.EventNodeRestored),
		nodeIdentity,
		nodeType,
		nodeGroup,
	).Err()
}

func (d *Directory) drainNodeWithLua(ctx context.Context, node sp.Node) error {
	result, err := d.client.Eval(ctx, drainNodeLua, []string{
		NodeKey(node.NodeIdentity),
		InvalidNodesKey(node.NodeType, node.NodeGroup),
		EventsStreamKey(),
	},
		string(sp.EventNodeDraining),
	).Text()
	if err != nil {
		return err
	}
	switch result {
	case "node_not_found":
		return sp.ErrNodeNotFound
	case "node_not_invalid":
		return sp.ErrNodeNotInvalid
	default:
		return nil
	}
}

func (d *Directory) unregisterNodeWithLua(ctx context.Context, node sp.Node, nodeSessionID string, guardPlacements bool) error {
	guard := "0"
	if guardPlacements {
		guard = "1"
	}
	result, err := d.client.Eval(ctx, unregisterNodeLua, []string{
		NodeKey(node.NodeIdentity),
		NodesKey(node.NodeType, node.NodeGroup),
		EventsStreamKey(),
		PlacementNodeKey(node.NodeIdentity),
		NodeHeartbeatKey(node.NodeType, node.NodeGroup),
	},
		nodeSessionID,
		string(sp.EventNodeUnregistered),
		guard,
	).Text()
	if err != nil {
		return err
	}
	switch result {
	case "node_not_found":
		return sp.ErrNodeNotFound
	case "invalid_node_session":
		return sp.ErrInvalidNodeSession
	case "node_has_placements":
		return sp.ErrNodeHasPlacements
	default:
		return nil
	}
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

func (d *Directory) getNodeFromMember(ctx context.Context, member string) (*sp.Node, error) {
	if strings.HasPrefix(member, "sp:") {
		value, err := d.client.Get(ctx, member).Bytes()
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
	return d.getNode(ctx, member)
}

func marshalRedisNode(node sp.Node) ([]byte, error) {
	return json.Marshal(redisNode{
		Node:             node,
		PlacementNodeKey: PlacementNodeKey(node.NodeIdentity),
	})
}

func normalizeNode(node *sp.Node) error {
	identity, err := sp.NewNodeIdentity(node.NodeType, node.NodeGroup, node.NodeName)
	if err != nil {
		return err
	}
	expected := identity.String()
	if node.NodeIdentity == "" {
		node.NodeIdentity = expected
	} else if node.NodeIdentity != expected {
		return fmt.Errorf("node identity mismatch: expected %q, got %q", expected, node.NodeIdentity)
	}
	if node.Status == "" {
		node.Status = sp.NodeStatusActive
	}
	if node.LastHeartbeatAt.IsZero() {
		node.LastHeartbeatAt = time.Now()
	}
	return nil
}

func parseCursor(cursor string) (int64, string, error) {
	rawScore, key, ok := strings.Cut(cursor, ":")
	if !ok || key == "" {
		return 0, "", fmt.Errorf("invalid placement cursor %q", cursor)
	}
	score, err := strconv.ParseInt(rawScore, 10, 64)
	if err != nil {
		return 0, "", err
	}
	return score, key, nil
}

func formatCursor(score int64, key string) string {
	return strconv.FormatInt(score, 10) + ":" + key
}
