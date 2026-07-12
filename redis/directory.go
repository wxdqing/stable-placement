package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type Directory struct {
	client goredis.UniversalClient
	mode   sp.StrategyMode
	config sp.NodeLeaseConfig
}

type redisNode struct {
	sp.Node
	PlacementNodeKey string
	NodeKey          string
}

func NewDirectory(client goredis.UniversalClient, mode sp.StrategyMode, config sp.NodeLeaseConfig) (*Directory, error) {
	if mode != sp.StrategyModeRedisRoundRobin {
		return nil, sp.ErrUnsupportedStrategyMode
	}
	if config.TTL <= 0 {
		return nil, sp.ErrInvalidNodeLeaseTTL
	}
	ttlMillis := config.TTL.Milliseconds()
	maxTTLMillis := time.Duration(1<<63 - 1).Milliseconds()
	if config.TTL%time.Millisecond != 0 && ttlMillis < maxTTLMillis {
		ttlMillis++
	}
	if ttlMillis <= 0 {
		ttlMillis = 1
	}
	config.TTL = time.Duration(ttlMillis) * time.Millisecond
	return &Directory{client: client, mode: mode, config: config}, nil
}

func (d *Directory) RegisterNode(ctx context.Context, node sp.Node) error {
	if err := normalizeNode(&node); err != nil {
		return err
	}
	encoded, err := json.Marshal(redisNode{Node: node, PlacementNodeKey: PlacementNodeKey(node.NodeIdentity), NodeKey: NodeKey(node.NodeIdentity)})
	if err != nil {
		return err
	}
	result, err := d.client.Eval(ctx, registerNodeLua, []string{NodeKey(node.NodeIdentity), NodesKey(node.NodeType, node.NodeGroup), NodeLeaseKey(node.NodeType, node.NodeGroup), EventsStreamKey()}, string(encoded), strconv.FormatInt(d.config.TTL.Milliseconds(), 10), NodeKey(node.NodeIdentity), string(sp.EventNodeRegistered)).Text()
	if err != nil {
		return err
	}
	return nodeResultError(result)
}

func (d *Directory) RenewNode(ctx context.Context, identity, session string) error {
	node, err := d.getNode(ctx, identity)
	if err != nil {
		return err
	}
	result, err := d.client.Eval(ctx, renewNodeLua, []string{NodeKey(identity), NodeLeaseKey(node.NodeType, node.NodeGroup)}, session, NodeKey(identity)).Text()
	if err != nil {
		return err
	}
	return nodeResultError(result)
}

func (d *Directory) ReplaceNodeSession(ctx context.Context, node sp.Node) (*sp.Node, error) {
	if err := normalizeNode(&node); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(redisNode{Node: node, PlacementNodeKey: PlacementNodeKey(node.NodeIdentity), NodeKey: NodeKey(node.NodeIdentity)})
	if err != nil {
		return nil, err
	}
	result, err := d.client.Eval(ctx, replaceNodeSessionLua, []string{NodeKey(node.NodeIdentity), NodesKey(node.NodeType, node.NodeGroup), NodeLeaseKey(node.NodeType, node.NodeGroup), EventsStreamKey()}, string(encoded), strconv.FormatInt(d.config.TTL.Milliseconds(), 10), NodeKey(node.NodeIdentity), string(sp.EventNodeReplaced)).Result()
	if err != nil {
		return nil, err
	}
	if s, ok := result.(string); ok {
		if e := nodeResultError(s); e != nil {
			return nil, e
		}
		return &sp.Node{}, nil
	}
	raw, ok := result.([]interface{})
	if !ok || len(raw) != 2 {
		return nil, fmt.Errorf("unexpected replace result %T", result)
	}
	if status := fmt.Sprint(raw[0]); status != "ok" {
		return nil, nodeResultError(status)
	}
	var old redisNode
	if fmt.Sprint(raw[1]) != "" {
		if err := json.Unmarshal([]byte(fmt.Sprint(raw[1])), &old); err != nil {
			return nil, err
		}
	}
	return &old.Node, nil
}

func (d *Directory) UnregisterNode(ctx context.Context, identity, session string) error {
	return d.unregister(ctx, identity, session, false)
}
func (d *Directory) CompleteDrain(ctx context.Context, identity, session string) error {
	return d.unregister(ctx, identity, session, true)
}
func (d *Directory) unregister(ctx context.Context, identity, session string, guard bool) error {
	node, err := d.getNode(ctx, identity)
	if err != nil {
		return err
	}
	result, err := d.client.Eval(ctx, unregisterNodeLua, []string{NodeKey(identity), NodesKey(node.NodeType, node.NodeGroup), NodeLeaseKey(node.NodeType, node.NodeGroup), PlacementNodeKey(identity), EventsStreamKey()}, session, NodeKey(identity), strconv.FormatBool(guard), string(sp.EventNodeUnregistered)).Text()
	if err != nil {
		return err
	}
	return nodeResultError(result)
}

func (d *Directory) FindNodes(ctx context.Context, nodeType, nodeGroup string) ([]sp.Node, error) {
	members, err := d.client.SMembers(ctx, NodesKey(nodeType, nodeGroup)).Result()
	if err != nil {
		return nil, err
	}
	nodes := make([]sp.Node, 0, len(members))
	for _, key := range members {
		raw, err := d.client.Get(ctx, key).Bytes()
		if errors.Is(err, goredis.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var node redisNode
		if err := json.Unmarshal(raw, &node); err != nil {
			return nil, err
		}
		nodes = append(nodes, node.Node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeIdentity < nodes[j].NodeIdentity })
	return nodes, nil
}

func (d *Directory) DrainNode(ctx context.Context, identity string) error {
	node, err := d.getNode(ctx, identity)
	if err != nil {
		return err
	}
	result, err := d.client.Eval(ctx, drainNodeLua, []string{NodeKey(identity), InvalidNodesKey(node.NodeType, node.NodeGroup), NodeLeaseKey(node.NodeType, node.NodeGroup), EventsStreamKey()}, node.NodeName, string(sp.EventNodeDraining), NodeKey(identity)).Text()
	if err != nil {
		return err
	}
	return nodeResultError(result)
}
func (d *Directory) MarkNodeInvalid(ctx context.Context, nodeType, nodeGroup, nodeName string) error {
	id, err := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	if err != nil {
		return err
	}
	_, err = d.client.Eval(ctx, markInvalidLua, []string{InvalidNodesKey(nodeType, nodeGroup), EventsStreamKey()}, nodeName, id.String(), nodeType, nodeGroup, string(sp.EventNodeMarkedInvalid)).Result()
	return err
}
func (d *Directory) RestoreNode(ctx context.Context, nodeType, nodeGroup, nodeName string) error {
	id, err := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	if err != nil {
		return err
	}
	_, err = d.client.Eval(ctx, restoreNodeLua, []string{InvalidNodesKey(nodeType, nodeGroup), EventsStreamKey()}, nodeName, id.String(), nodeType, nodeGroup, string(sp.EventNodeRestored)).Result()
	return err
}
func (d *Directory) ListInvalidNodes(ctx context.Context, nodeType, nodeGroup string) ([]string, error) {
	v, e := d.client.SMembers(ctx, InvalidNodesKey(nodeType, nodeGroup)).Result()
	sort.Strings(v)
	return v, e
}

func (d *Directory) Lookup(ctx context.Context, key sp.GrainKey) (*sp.PlacementRoute, error) {
	requestStart := time.Now()
	raw, p, err := d.getPlacementRaw(ctx, key)
	if err != nil {
		if errors.Is(err, sp.ErrPlacementNotFound) {
			return nil, sp.ErrPlacementNotFound
		}
		return nil, err
	}
	result, err := d.client.Eval(ctx, lookupLua, []string{PlacementKey(key), NodeKey(p.NodeIdentity), NodeLeaseKeyForNode(*p)}, string(raw), NodeKey(p.NodeIdentity), key.String()).Result()
	if err != nil {
		return nil, err
	}
	items, ok := result.([]interface{})
	if !ok || len(items) != 3 {
		return nil, sp.ErrPlacementNotFound
	}
	var current sp.Placement
	if err := json.Unmarshal([]byte(fmt.Sprint(items[0])), &current); err != nil {
		return nil, err
	}
	leaseVersion, err := strconv.ParseInt(fmt.Sprint(items[1]), 10, 64)
	if err != nil {
		return nil, err
	}
	remaining, err := strconv.ParseInt(fmt.Sprint(items[2]), 10, 64)
	if err != nil {
		return nil, err
	}
	if remaining <= 0 || remaining > time.Duration(1<<63-1).Milliseconds() {
		return nil, sp.ErrPlacementNotFound
	}
	validUntil := requestStart.Add(time.Duration(remaining) * time.Millisecond)
	if !time.Now().Before(validUntil) {
		return nil, sp.ErrPlacementNotFound
	}
	return &sp.PlacementRoute{GrainKey: current.GrainKey, NodeIdentity: current.NodeIdentity, OwnerNodeSessionID: current.OwnerNodeSessionID, Version: current.Version, Status: current.Status, NodeLeaseVersion: leaseVersion, ValidUntil: validUntil}, nil
}

func NodeLeaseKeyForNode(p sp.Placement) string {
	id := sp.NodeIdentity(p.NodeIdentity)
	return NodeLeaseKey(id.NodeType(), id.NodeGroup())
}

func (d *Directory) Allocate(ctx context.Context, cmd sp.AllocateCommand) (*sp.Placement, error) {
	key, err := sp.NewGrainKey(cmd.Kind, cmd.GrainID)
	if err != nil {
		return nil, err
	}
	oldRaw := ""
	oldIndex := PlacementNodeKey("")
	oldNode := NodeKey("")
	if raw, p, e := d.getPlacementRaw(ctx, key); e == nil {
		oldRaw = string(raw)
		oldIndex = PlacementNodeKey(p.NodeIdentity)
		oldNode = NodeKey(p.NodeIdentity)
	} else if !errors.Is(e, sp.ErrPlacementNotFound) {
		return nil, e
	}
	ownerLease := NodeLeaseKey("", "")
	if oldRaw != "" {
		id := sp.NodeIdentity(oldNodeIdentity(oldNode, oldRaw))
		ownerLease = NodeLeaseKey(id.NodeType(), id.NodeGroup())
	}
	result, err := d.client.Eval(ctx, allocateLua, []string{PlacementKey(key), NodesKey(cmd.TargetNodeType, cmd.TargetNodeGroup), InvalidNodesKey(cmd.TargetNodeType, cmd.TargetNodeGroup), StrategyRoundRobinKey(cmd.TargetNodeType, cmd.TargetNodeGroup), NodeLeaseKey(cmd.TargetNodeType, cmd.TargetNodeGroup), SequenceKey(), EventsStreamKey(), oldIndex, oldNode, ownerLease}, cmd.GrainID, cmd.Kind, key.String(), oldRaw, string(sp.EventPlacementCreated), cmd.TargetNodeType, cmd.TargetNodeGroup).Result()
	if err != nil {
		return nil, err
	}
	if s, ok := result.(string); ok {
		switch s {
		case "owner_unavailable":
			return nil, sp.ErrPlacementOwnerUnavailable
		case "no_available_node":
			return nil, sp.ErrNoAvailableNode
		case "conflict":
			return nil, sp.ErrVersionConflict
		}
	}
	return decodePlacement(result)
}

func (d *Directory) Renew(ctx context.Context, cmd sp.RenewCommand) (*sp.Placement, error) {
	raw, p, err := d.getPlacementRaw(ctx, cmd.GrainKey)
	if err != nil {
		return nil, err
	}
	result, err := d.client.Eval(ctx, renewPlacementLua, []string{PlacementKey(cmd.GrainKey), NodeKey(p.NodeIdentity), NodeLeaseKeyForNode(*p), AuditStreamKey()}, string(raw), cmd.NodeIdentity, cmd.NodeSessionID, strconv.FormatInt(cmd.PlacementVersion, 10), string(sp.EventPlacementRenewed), cmd.GrainKey.String()).Result()
	if err != nil {
		return nil, err
	}
	if s, ok := result.(string); ok && (len(s) == 0 || s[0] != '{') {
		if e := placementResultError(s); e != nil {
			return nil, e
		}
	}
	return decodePlacement(result)
}

func (d *Directory) Release(ctx context.Context, cmd sp.ReleaseCommand) error {
	_, err := d.mutate(ctx, "release", cmd.GrainKey, cmd.NodeIdentity, "", cmd.NodeSessionID, cmd.PlacementVersion, sp.EventPlacementReleased)
	return err
}
func (d *Directory) Transfer(ctx context.Context, cmd sp.TransferCommand) (*sp.Placement, error) {
	return d.mutate(ctx, "transfer", cmd.GrainKey, cmd.FromNodeIdentity, cmd.ToNodeIdentity, "", cmd.PlacementVersion, sp.EventPlacementTransferred)
}
func (d *Directory) Recover(ctx context.Context, cmd sp.RecoverCommand) (*sp.Placement, error) {
	return d.mutate(ctx, "recover", cmd.GrainKey, "", cmd.NewNodeIdentity, "", cmd.PlacementVersion, sp.EventPlacementRecovered)
}
func (d *Directory) mutate(ctx context.Context, mode string, key sp.GrainKey, from, target, session string, version int64, event sp.EventType) (*sp.Placement, error) {
	raw, p, err := d.getPlacementRaw(ctx, key)
	if err != nil {
		return nil, err
	}
	targetKey := NodeKey(target)
	newIndex := PlacementNodeKey(target)
	leaseKey := NodeLeaseKey("", "")
	if target != "" {
		id := sp.NodeIdentity(target)
		if id.NodeType() == "" {
			return nil, fmt.Errorf("invalid node identity %q", target)
		}
		leaseKey = NodeLeaseKey(id.NodeType(), id.NodeGroup())
	}
	ownerID := sp.NodeIdentity(p.NodeIdentity)
	ownerLeaseKey := NodeLeaseKey(ownerID.NodeType(), ownerID.NodeGroup())
	targetID := sp.NodeIdentity(target)
	targetInvalidKey := InvalidNodesKey(targetID.NodeType(), targetID.NodeGroup())
	result, err := d.client.Eval(ctx, mutationLua, []string{PlacementKey(key), NodeKey(p.NodeIdentity), targetKey, PlacementNodeKey(p.NodeIdentity), newIndex, leaseKey, EventsStreamKey(), ownerLeaseKey, targetInvalidKey, SequenceKey()}, mode, string(raw), from, target, session, strconv.FormatInt(version, 10), string(event), key.String()).Result()
	if err != nil {
		return nil, err
	}
	if s, ok := result.(string); ok && (len(s) == 0 || s[0] != '{') {
		if e := placementResultError(s); e != nil {
			return nil, e
		}
	}
	return decodePlacement(result)
}

func (d *Directory) Exists(ctx context.Context, key sp.GrainKey) (bool, error) {
	_, err := d.Lookup(ctx, key)
	if errors.Is(err, sp.ErrPlacementNotFound) {
		return false, nil
	}
	return err == nil, err
}
func (d *Directory) FindByNode(ctx context.Context, q sp.FindByNodeQuery) (sp.PlacementPage, error) {
	if q.Cursor != "" {
		n, err := strconv.Atoi(q.Cursor)
		if err != nil || n < 0 {
			return sp.PlacementPage{}, fmt.Errorf("invalid cursor")
		}
	}
	start := int64(0)
	if q.Cursor != "" {
		start, _ = strconv.ParseInt(q.Cursor, 10, 64)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	members, err := d.client.ZRange(ctx, PlacementNodeKey(q.NodeIdentity), start, start+int64(limit)-1).Result()
	if err != nil {
		return sp.PlacementPage{}, err
	}
	status := q.Status
	if status == "" {
		status = sp.PlacementStatusActive
	}
	page := sp.PlacementPage{}
	for _, member := range members {
		p, err := d.getPlacement(ctx, sp.GrainKey(member))
		if errors.Is(err, sp.ErrPlacementNotFound) {
			continue
		}
		if err != nil {
			return page, err
		}
		if p.Status == status {
			page.Placements = append(page.Placements, *p)
		}
	}
	if len(members) == limit {
		page.NextCursor = strconv.FormatInt(start+int64(len(members)), 10)
	}
	return page, nil
}

func (d *Directory) getNode(ctx context.Context, identity string) (*sp.Node, error) {
	raw, err := d.client.Get(ctx, NodeKey(identity)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, sp.ErrNodeNotFound
	}
	if err != nil {
		return nil, err
	}
	var node redisNode
	if err := json.Unmarshal(raw, &node); err != nil {
		return nil, err
	}
	return &node.Node, nil
}
func (d *Directory) getPlacement(ctx context.Context, key sp.GrainKey) (*sp.Placement, error) {
	_, p, e := d.getPlacementRaw(ctx, key)
	return p, e
}
func (d *Directory) getPlacementRaw(ctx context.Context, key sp.GrainKey) ([]byte, *sp.Placement, error) {
	raw, err := d.client.Get(ctx, PlacementKey(key)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, nil, sp.ErrPlacementNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	var p sp.Placement
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, nil, err
	}
	return raw, &p, nil
}
func decodePlacement(value interface{}) (*sp.Placement, error) {
	raw := fmt.Sprint(value)
	var p sp.Placement
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func oldNodeIdentity(_ string, raw string) string {
	var placement sp.Placement
	_ = json.Unmarshal([]byte(raw), &placement)
	return placement.NodeIdentity
}

func normalizeNode(node *sp.Node) error {
	if node.NodeSessionID == "" {
		return fmt.Errorf("node session ID is empty")
	}
	id, err := sp.NewNodeIdentity(node.NodeType, node.NodeGroup, node.NodeName)
	if err != nil {
		return err
	}
	if node.NodeIdentity != id.String() {
		return fmt.Errorf("node identity mismatch: expected %q, got %q", id.String(), node.NodeIdentity)
	}
	return nil
}
func nodeResultError(s string) error {
	switch s {
	case "ok":
		return nil
	case "node_not_found":
		return sp.ErrNodeNotFound
	case "invalid_node_session":
		return sp.ErrInvalidNodeSession
	case "node_lease_expired":
		return sp.ErrNodeLeaseExpired
	case "node_has_placements":
		return sp.ErrNodeHasPlacements
	case "node_not_invalid":
		return sp.ErrNodeNotInvalid
	}
	return fmt.Errorf("unexpected redis result %q", s)
}
func placementResultError(s string) error {
	switch s {
	case "placement_not_found":
		return sp.ErrPlacementNotFound
	case "invalid_owner":
		return sp.ErrInvalidOwner
	case "invalid_node_session":
		return sp.ErrInvalidNodeSession
	case "version_conflict":
		return sp.ErrVersionConflict
	case "owner_unavailable":
		return sp.ErrPlacementOwnerUnavailable
	case "node_lease_expired":
		return sp.ErrNodeLeaseExpired
	case "no_available_node":
		return sp.ErrNoAvailableNode
	case "not_recoverable":
		return sp.ErrPlacementNotRecoverable
	}
	return fmt.Errorf("unexpected redis result %q", s)
}
