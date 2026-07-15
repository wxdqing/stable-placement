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
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type Directory struct {
	client         goredis.UniversalClient
	mode           sp.StrategyMode
	config         sp.NodeLeaseConfig
	resourceConfig sp.ResourceAwareConfig
}

type redisNode struct {
	sp.Node
	PlacementNodeKey string
	NodeKey          string
}

type redisPlacement struct {
	GrainID             string
	Kind                string
	GrainKey            sp.GrainKey
	PlacementID         string
	NodeIdentity        string
	OwnerNodeSessionID  string
	Version             int64
	Status              sp.PlacementStatus
	CreateTimeUnixMilli int64
	UpdateTimeUnixMilli int64
}

func redisPlacementFromPlacement(placement sp.Placement) redisPlacement {
	return redisPlacement{
		GrainID: placement.GrainID, Kind: placement.Kind, GrainKey: placement.GrainKey, PlacementID: placement.PlacementID,
		NodeIdentity: placement.NodeIdentity, OwnerNodeSessionID: placement.OwnerNodeSessionID,
		Version: placement.Version, Status: placement.Status,
		CreateTimeUnixMilli: placement.CreateTime.UnixMilli(), UpdateTimeUnixMilli: placement.UpdateTime.UnixMilli(),
	}
}

func (placement redisPlacement) placement() sp.Placement {
	return sp.Placement{
		GrainID: placement.GrainID, Kind: placement.Kind, GrainKey: placement.GrainKey, PlacementID: placement.PlacementID,
		NodeIdentity: placement.NodeIdentity, OwnerNodeSessionID: placement.OwnerNodeSessionID,
		Version: placement.Version, Status: placement.Status,
		CreateTime: time.UnixMilli(placement.CreateTimeUnixMilli), UpdateTime: time.UnixMilli(placement.UpdateTimeUnixMilli),
	}
}

const maxPlacementIndexScore = int64(1<<53 - 1)

func NewDirectory(client goredis.UniversalClient, mode sp.StrategyMode, config sp.NodeLeaseConfig, resourceConfigs ...sp.ResourceAwareConfig) (*Directory, error) {
	if mode != sp.StrategyModeRedisRoundRobin && mode != sp.StrategyModeRedisResourceAware {
		return nil, sp.ErrUnsupportedStrategyMode
	}
	if config.TTL <= 0 {
		return nil, sp.ErrInvalidNodeLeaseTTL
	}
	if len(resourceConfigs) > 1 {
		return nil, fmt.Errorf("%w: multiple resource-aware configs", sp.ErrPlacementConfigInvalid)
	}
	resourceConfig := sp.ResourceAwareConfig{}
	if len(resourceConfigs) == 1 {
		resourceConfig = resourceConfigs[0]
	}
	resourceConfig, err := sp.NormalizeResourceAwareConfig(resourceConfig)
	if err != nil {
		return nil, err
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
	return &Directory{client: client, mode: mode, config: config, resourceConfig: resourceConfig}, nil
}

func (d *Directory) RegisterNode(ctx context.Context, node sp.Node) (sp.NodeLeaseGrant, error) {
	if err := normalizeNode(&node); err != nil {
		return sp.NodeLeaseGrant{}, err
	}
	node.Metrics = sp.NodeMetrics{}
	encoded, err := json.Marshal(redisNode{Node: node, PlacementNodeKey: PlacementNodeKey(node.NodeIdentity), NodeKey: NodeKey(node.NodeIdentity)})
	if err != nil {
		return sp.NodeLeaseGrant{}, err
	}
	requestStart := time.Now()
	result, err := d.client.Eval(ctx, registerNodeLua, []string{NodeKey(node.NodeIdentity), NodesKey(node.NodeType, node.NodeGroup), NodeLeaseKey(node.NodeType, node.NodeGroup), EventsStreamKey()}, string(encoded), strconv.FormatInt(d.config.TTL.Milliseconds(), 10), NodeKey(node.NodeIdentity), string(sp.EventNodeRegistered)).Result()
	if err != nil {
		return sp.NodeLeaseGrant{}, err
	}
	return nodeLeaseGrantResult(requestStart, result, 1, 2)
}

func (d *Directory) RenewNode(ctx context.Context, cmd sp.RenewNodeCommand) (sp.NodeLeaseGrant, error) {
	metricsJSON := ""
	if cmd.Metrics != nil {
		encoded, err := json.Marshal(cmd.Metrics)
		if err != nil {
			return sp.NodeLeaseGrant{}, err
		}
		metricsJSON = string(encoded)
	}
	node, err := d.getNode(ctx, cmd.NodeIdentity)
	if err != nil {
		return sp.NodeLeaseGrant{}, err
	}
	requestStart := time.Now()
	result, err := d.client.Eval(ctx, renewNodeLua, []string{NodeKey(cmd.NodeIdentity), NodeLeaseKey(node.NodeType, node.NodeGroup)}, cmd.NodeSessionID, NodeKey(cmd.NodeIdentity), metricsJSON).Result()
	if err != nil {
		return sp.NodeLeaseGrant{}, err
	}
	return nodeLeaseGrantResult(requestStart, result, 1, 2)
}

func (d *Directory) ReplaceNodeSession(ctx context.Context, node sp.Node) (*sp.Node, sp.NodeLeaseGrant, error) {
	if err := normalizeNode(&node); err != nil {
		return nil, sp.NodeLeaseGrant{}, err
	}
	node.Metrics = sp.NodeMetrics{}
	encoded, err := json.Marshal(redisNode{Node: node, PlacementNodeKey: PlacementNodeKey(node.NodeIdentity), NodeKey: NodeKey(node.NodeIdentity)})
	if err != nil {
		return nil, sp.NodeLeaseGrant{}, err
	}
	requestStart := time.Now()
	result, err := d.client.Eval(ctx, replaceNodeSessionLua, []string{NodeKey(node.NodeIdentity), NodesKey(node.NodeType, node.NodeGroup), NodeLeaseKey(node.NodeType, node.NodeGroup), EventsStreamKey()}, string(encoded), strconv.FormatInt(d.config.TTL.Milliseconds(), 10), NodeKey(node.NodeIdentity), string(sp.EventNodeReplaced)).Result()
	if err != nil {
		return nil, sp.NodeLeaseGrant{}, err
	}
	if s, ok := result.(string); ok {
		if e := nodeResultError(s); e != nil {
			return nil, sp.NodeLeaseGrant{}, e
		}
		return nil, sp.NodeLeaseGrant{}, fmt.Errorf("unexpected replace result %q", s)
	}
	raw, ok := result.([]interface{})
	if !ok || len(raw) != 4 {
		return nil, sp.NodeLeaseGrant{}, fmt.Errorf("unexpected replace result %T", result)
	}
	if status := fmt.Sprint(raw[0]); status != "ok" {
		return nil, sp.NodeLeaseGrant{}, nodeResultError(status)
	}
	var old redisNode
	if fmt.Sprint(raw[1]) != "" {
		if err := json.Unmarshal([]byte(fmt.Sprint(raw[1])), &old); err != nil {
			return nil, sp.NodeLeaseGrant{}, err
		}
	}
	grant, err := nodeLeaseGrantResult(requestStart, []interface{}{raw[0], raw[2], raw[3]}, 1, 2)
	if err != nil {
		return nil, sp.NodeLeaseGrant{}, err
	}
	return &old.Node, grant, nil
}

func nodeLeaseGrantResult(requestStart time.Time, result interface{}, versionIndex, ttlIndex int) (sp.NodeLeaseGrant, error) {
	if status, ok := result.(string); ok {
		return sp.NodeLeaseGrant{}, nodeResultError(status)
	}
	items, ok := result.([]interface{})
	if !ok || len(items) <= versionIndex || len(items) <= ttlIndex || fmt.Sprint(items[0]) != "ok" {
		return sp.NodeLeaseGrant{}, fmt.Errorf("unexpected node lease result %T", result)
	}
	version, err := strconv.ParseInt(fmt.Sprint(items[versionIndex]), 10, 64)
	if err != nil || version <= 0 {
		return sp.NodeLeaseGrant{}, fmt.Errorf("invalid node lease version %q", items[versionIndex])
	}
	remainingTTLMillis, err := strconv.ParseInt(fmt.Sprint(items[ttlIndex]), 10, 64)
	if err != nil || remainingTTLMillis <= 0 {
		return sp.NodeLeaseGrant{}, sp.ErrNodeLeaseExpired
	}
	return sp.NodeLeaseGrant{Version: version, ValidUntil: requestStart.Add(time.Duration(remainingTTLMillis) * time.Millisecond)}, nil
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
	var currentWire redisPlacement
	if err := json.Unmarshal([]byte(fmt.Sprint(items[0])), &currentWire); err != nil {
		return nil, err
	}
	current := currentWire.placement()
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
	return &sp.PlacementRoute{GrainKey: current.GrainKey, PlacementID: current.PlacementID, NodeIdentity: current.NodeIdentity, OwnerNodeSessionID: current.OwnerNodeSessionID, Version: current.Version, Status: current.Status, NodeLeaseVersion: leaseVersion, ValidUntil: validUntil}, nil
}

func (d *Directory) ResolveRoute(ctx context.Context, cmd sp.ResolveRouteCommand) (*sp.PlacementRoute, error) {
	key, err := sp.NewGrainKey(cmd.Kind, cmd.GrainID)
	if err != nil {
		return nil, err
	}
	if _, err := sp.NewNodeIdentity(cmd.TargetNodeType, cmd.TargetNodeGroup, "target"); err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		route, err := d.resolveRouteOnce(ctx, key, cmd)
		if !errors.Is(err, sp.ErrVersionConflict) {
			return route, err
		}
	}
	return nil, sp.ErrVersionConflict
}

func (d *Directory) resolveRouteOnce(ctx context.Context, key sp.GrainKey, cmd sp.ResolveRouteCommand) (*sp.PlacementRoute, error) {
	placementID, err := sp.NewPlacementID()
	if err != nil {
		return nil, err
	}
	oldRaw := ""
	oldIndex := PlacementNodeKey("")
	oldNode := NodeKey("")
	ownerLease := NodeLeaseKey("", "")
	ownerType, ownerGroup, ownerName := "", "", ""
	if raw, placement, err := d.getPlacementRaw(ctx, key); err == nil {
		oldRaw = string(raw)
		oldIndex = PlacementNodeKey(placement.NodeIdentity)
		oldNode = NodeKey(placement.NodeIdentity)
		ownerID := sp.NodeIdentity(placement.NodeIdentity)
		ownerType, ownerGroup, ownerName = ownerID.NodeType(), ownerID.NodeGroup(), ownerID.NodeName()
		if ownerType == "" || ownerGroup == "" || ownerName == "" {
			return nil, fmt.Errorf("invalid placement owner identity %q", placement.NodeIdentity)
		}
		ownerLease = NodeLeaseKey(ownerType, ownerGroup)
	} else if !errors.Is(err, sp.ErrPlacementNotFound) {
		return nil, err
	}
	requestStart := time.Now()
	args := []interface{}{
		cmd.GrainID, cmd.Kind, key.String(), oldRaw, string(sp.EventPlacementCreated),
		cmd.TargetNodeType, cmd.TargetNodeGroup, string(sp.EventPlacementRecovered),
		ownerType, ownerGroup, ownerName,
	}
	args = append(args, d.resourceStrategyArgs()...)
	args = append(args, placementID)
	result, err := d.client.Eval(ctx, resolveRouteLua, []string{
		PlacementKey(key), NodesKey(cmd.TargetNodeType, cmd.TargetNodeGroup),
		InvalidNodesKey(cmd.TargetNodeType, cmd.TargetNodeGroup), StrategyRoundRobinKey(cmd.TargetNodeType, cmd.TargetNodeGroup),
		NodeLeaseKey(cmd.TargetNodeType, cmd.TargetNodeGroup), SequenceKey(), EventsStreamKey(),
		oldIndex, oldNode, ownerLease,
	}, args...).Result()
	if err != nil {
		return nil, err
	}
	if status, ok := result.(string); ok {
		switch status {
		case "conflict":
			return nil, sp.ErrVersionConflict
		case "target_mismatch":
			return nil, sp.ErrPlacementTargetMismatch
		case "owner_unavailable":
			return nil, sp.ErrPlacementOwnerUnavailable
		case "no_available_node":
			return nil, sp.ErrNoAvailableNode
		default:
			return nil, fmt.Errorf("unexpected resolve route result %q", status)
		}
	}
	items, ok := result.([]interface{})
	if !ok || len(items) != 3 {
		return nil, fmt.Errorf("unexpected resolve route result %T", result)
	}
	var wire redisPlacement
	if err := json.Unmarshal([]byte(fmt.Sprint(items[0])), &wire); err != nil {
		return nil, err
	}
	leaseVersion, err := strconv.ParseInt(fmt.Sprint(items[1]), 10, 64)
	if err != nil || leaseVersion <= 0 {
		return nil, fmt.Errorf("invalid route lease version %q", items[1])
	}
	remaining, err := strconv.ParseInt(fmt.Sprint(items[2]), 10, 64)
	if err != nil || remaining <= 0 || remaining > time.Duration(1<<63-1).Milliseconds() {
		return nil, sp.ErrPlacementOwnerUnavailable
	}
	validUntil := requestStart.Add(time.Duration(remaining) * time.Millisecond)
	if !time.Now().Before(validUntil) {
		return nil, sp.ErrPlacementOwnerUnavailable
	}
	placement := wire.placement()
	return &sp.PlacementRoute{
		GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: placement.NodeIdentity,
		OwnerNodeSessionID: placement.OwnerNodeSessionID, Version: placement.Version,
		Status: placement.Status, NodeLeaseVersion: leaseVersion, ValidUntil: validUntil,
	}, nil
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
	placementID, err := sp.NewPlacementID()
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
	args := []interface{}{cmd.GrainID, cmd.Kind, key.String(), oldRaw, string(sp.EventPlacementCreated), cmd.TargetNodeType, cmd.TargetNodeGroup}
	args = append(args, d.resourceStrategyArgs()...)
	args = append(args, placementID)
	result, err := d.client.Eval(ctx, allocateLua, []string{PlacementKey(key), NodesKey(cmd.TargetNodeType, cmd.TargetNodeGroup), InvalidNodesKey(cmd.TargetNodeType, cmd.TargetNodeGroup), StrategyRoundRobinKey(cmd.TargetNodeType, cmd.TargetNodeGroup), NodeLeaseKey(cmd.TargetNodeType, cmd.TargetNodeGroup), SequenceKey(), EventsStreamKey(), oldIndex, oldNode, ownerLease}, args...).Result()
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

func (d *Directory) resourceStrategyArgs() []interface{} {
	return []interface{}{
		string(d.mode),
		strconv.FormatInt(d.resourceConfig.MetricsMaxAge.Milliseconds(), 10),
		strconv.FormatInt(d.resourceConfig.MinMemoryAvailableBytes, 10),
		strconv.FormatInt(d.resourceConfig.MinCPUAvailableMilliCores, 10),
		strconv.FormatInt(d.resourceConfig.MaxGoroutines, 10),
	}
}

func (d *Directory) Renew(ctx context.Context, cmd sp.RenewCommand) (*sp.Placement, error) {
	raw, p, err := d.getPlacementRaw(ctx, cmd.GrainKey)
	if err != nil {
		return nil, err
	}
	result, err := d.client.Eval(ctx, renewPlacementLua, []string{PlacementKey(cmd.GrainKey), NodeKey(p.NodeIdentity), NodeLeaseKeyForNode(*p), AuditStreamKey()}, string(raw), cmd.NodeIdentity, cmd.NodeSessionID, strconv.FormatInt(cmd.PlacementVersion, 10), string(sp.EventPlacementRenewed), cmd.GrainKey.String(), cmd.PlacementID).Result()
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
	_, err := d.mutate(ctx, "release", cmd.GrainKey, cmd.PlacementID, cmd.NodeIdentity, "", cmd.NodeSessionID, cmd.PlacementVersion, sp.EventPlacementReleased)
	return err
}
func (d *Directory) Transfer(ctx context.Context, cmd sp.TransferCommand) (*sp.Placement, error) {
	return d.mutate(ctx, "transfer", cmd.GrainKey, cmd.PlacementID, cmd.FromNodeIdentity, cmd.ToNodeIdentity, "", cmd.PlacementVersion, sp.EventPlacementTransferred)
}
func (d *Directory) Recover(ctx context.Context, cmd sp.RecoverCommand) (*sp.Placement, error) {
	return d.mutate(ctx, "recover", cmd.GrainKey, cmd.PlacementID, "", cmd.NewNodeIdentity, "", cmd.PlacementVersion, sp.EventPlacementRecovered)
}
func (d *Directory) mutate(ctx context.Context, mode string, key sp.GrainKey, placementID, from, target, session string, version int64, event sp.EventType) (*sp.Placement, error) {
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
	result, err := d.client.Eval(ctx, mutationLua, []string{PlacementKey(key), NodeKey(p.NodeIdentity), targetKey, PlacementNodeKey(p.NodeIdentity), newIndex, leaseKey, EventsStreamKey(), ownerLeaseKey, targetInvalidKey, SequenceKey()}, mode, string(raw), from, target, session, strconv.FormatInt(version, 10), string(event), key.String(), ownerID.NodeType(), ownerID.NodeGroup(), ownerID.NodeName(), targetID.NodeType(), targetID.NodeGroup(), targetID.NodeName(), placementID).Result()
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
		values, err := d.client.ZRangeByScoreWithScores(ctx, indexKey, &goredis.ZRangeBy{Min: min, Max: "+inf", Count: batchSize}).Result()
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
				return sp.PlacementPage{Placements: placements[:limit], NextCursor: formatCursor(placementScores[limit-1], placementKeys[limit-1])}, nil
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
func formatCursor(score int64, key string) string { return strconv.FormatInt(score, 10) + ":" + key }

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
	var wire redisPlacement
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, nil, err
	}
	p := wire.placement()
	return raw, &p, nil
}
func decodePlacement(value interface{}) (*sp.Placement, error) {
	raw := fmt.Sprint(value)
	var wire redisPlacement
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		return nil, err
	}
	p := wire.placement()
	return &p, nil
}

func oldNodeIdentity(_ string, raw string) string {
	var placement redisPlacement
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
	case "invalid_metrics":
		return fmt.Errorf("%w: node metrics must not be negative or overflow Redis integers", sp.ErrPlacementConfigInvalid)
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
