package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

type nowFunc func() time.Time

type NodeRegistry struct {
	mu            sync.RWMutex
	nodes         map[string]sp.Node
	invalid       map[string]map[string]struct{}
	publisher     sp.EventPublisher
	config        sp.NodeLeaseConfig
	now           nowFunc
	completeDrain func(string, string) (sp.PlacementEvent, error)
}

func NewNodeRegistry(publisher sp.EventPublisher, config sp.NodeLeaseConfig) (*NodeRegistry, error) {
	return newNodeRegistry(publisher, config, time.Now)
}

func newNodeRegistry(publisher sp.EventPublisher, config sp.NodeLeaseConfig, now nowFunc) (*NodeRegistry, error) {
	if config.TTL <= 0 {
		return nil, sp.ErrInvalidNodeLeaseTTL
	}
	ttlMillis := config.TTL.Milliseconds()
	maxTTLMillis := time.Duration(1<<63 - 1).Milliseconds()
	if config.TTL%time.Millisecond != 0 && ttlMillis < maxTTLMillis {
		ttlMillis++
	}
	config.TTL = time.Duration(ttlMillis) * time.Millisecond
	return &NodeRegistry{
		nodes:     make(map[string]sp.Node),
		invalid:   make(map[string]map[string]struct{}),
		publisher: publisher,
		config:    config,
		now:       now,
	}, nil
}

func (r *NodeRegistry) RegisterNode(ctx context.Context, node sp.Node) (sp.NodeLeaseGrant, error) {
	if err := normalizeNodeIdentity(&node); err != nil {
		return sp.NodeLeaseGrant{}, err
	}
	now := r.now()
	r.mu.Lock()
	if existing, ok := r.nodes[node.NodeIdentity]; ok {
		if existing.NodeSessionID != node.NodeSessionID {
			r.mu.Unlock()
			return sp.NodeLeaseGrant{}, sp.ErrInvalidNodeSession
		}
		if existing.Status == sp.NodeStatusOffline || leaseExpired(existing, now) {
			r.mu.Unlock()
			return sp.NodeLeaseGrant{}, sp.ErrNodeLeaseExpired
		}
		r.mu.Unlock()
		return leaseGrant(existing, now), nil
	}
	node.Status = sp.NodeStatusActive
	node.Lease = r.newLease(now)
	r.nodes[node.NodeIdentity] = node
	event := r.nodeEvent(sp.EventNodeRegistered, node, now)
	r.mu.Unlock()
	if err := r.publish(ctx, event); err != nil {
		return sp.NodeLeaseGrant{}, err
	}
	return leaseGrant(node, now), nil
}

func (r *NodeRegistry) RenewNode(_ context.Context, nodeIdentity string, nodeSessionID string) (sp.NodeLeaseGrant, error) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodes[nodeIdentity]
	if !ok || node.Status == sp.NodeStatusOffline {
		return sp.NodeLeaseGrant{}, sp.ErrNodeNotFound
	}
	if node.NodeSessionID != nodeSessionID {
		return sp.NodeLeaseGrant{}, sp.ErrInvalidNodeSession
	}
	if leaseExpired(node, now) {
		return sp.NodeLeaseGrant{}, sp.ErrNodeLeaseExpired
	}
	node.Lease.Version++
	newExpiry := now.Add(time.Duration(node.Lease.TTLMillis) * time.Millisecond).UnixMilli()
	if newExpiry > node.Lease.ExpireAtUnixMilli {
		node.Lease.ExpireAtUnixMilli = newExpiry
	}
	r.nodes[nodeIdentity] = node
	return leaseGrant(node, now), nil
}

func (r *NodeRegistry) UnregisterNode(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	event, err := r.unregisterNode(nodeIdentity, nodeSessionID, nil)
	if err != nil {
		return err
	}
	return r.publish(ctx, event)
}

func (r *NodeRegistry) unregisterNode(nodeIdentity string, nodeSessionID string, hasActivePlacements func() bool) (sp.PlacementEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodes[nodeIdentity]
	if !ok {
		return sp.PlacementEvent{}, sp.ErrNodeNotFound
	}
	if node.NodeSessionID != nodeSessionID {
		return sp.PlacementEvent{}, sp.ErrInvalidNodeSession
	}
	if hasActivePlacements != nil && hasActivePlacements() {
		return sp.PlacementEvent{}, sp.ErrNodeHasPlacements
	}
	delete(r.nodes, nodeIdentity)
	return r.nodeEvent(sp.EventNodeUnregistered, node, r.now()), nil
}

func (r *NodeRegistry) ReplaceNodeSession(ctx context.Context, node sp.Node) (*sp.Node, sp.NodeLeaseGrant, error) {
	if err := normalizeNodeIdentity(&node); err != nil {
		return nil, sp.NodeLeaseGrant{}, err
	}
	now := r.now()
	r.mu.Lock()
	old, ok := r.nodes[node.NodeIdentity]
	if ok && old.NodeSessionID == node.NodeSessionID {
		r.mu.Unlock()
		return nil, sp.NodeLeaseGrant{}, sp.ErrInvalidNodeSession
	}
	node.Status = sp.NodeStatusActive
	node.Lease = r.newLease(now)
	r.nodes[node.NodeIdentity] = node
	event := r.nodeEvent(sp.EventNodeReplaced, node, now)
	r.mu.Unlock()
	if err := r.publish(ctx, event); err != nil {
		return nil, sp.NodeLeaseGrant{}, err
	}
	return &old, leaseGrant(node, now), nil
}

func (r *NodeRegistry) ExpireNodeLeases(ctx context.Context, nodeType, nodeGroup string, limit int64) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	now := r.now()
	r.mu.Lock()
	identities := make([]string, 0)
	for identity, node := range r.nodes {
		if node.NodeType == nodeType && node.NodeGroup == nodeGroup && node.Status != sp.NodeStatusOffline && leaseExpired(node, now) {
			identities = append(identities, identity)
		}
	}
	sort.Strings(identities)
	if int64(len(identities)) > limit {
		identities = identities[:limit]
	}
	events := make([]sp.PlacementEvent, 0, len(identities))
	for _, identity := range identities {
		node := r.nodes[identity]
		// The node is re-read in the mutation critical section, so a renewed lease
		// cannot be expired from a stale candidate snapshot.
		if node.Status == sp.NodeStatusOffline || !leaseExpired(node, now) {
			continue
		}
		node.Status = sp.NodeStatusOffline
		r.nodes[identity] = node
		events = append(events, r.nodeEvent(sp.EventNodeLeaseExpired, node, now))
	}
	r.mu.Unlock()
	for _, event := range events {
		if err := r.publish(ctx, event); err != nil {
			return len(events), err
		}
	}
	return len(events), nil
}

func (r *NodeRegistry) FindNodes(_ context.Context, nodeType string, nodeGroup string) ([]sp.Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var nodes []sp.Node
	for _, node := range r.nodes {
		if node.NodeType == nodeType && node.NodeGroup == nodeGroup {
			nodes = append(nodes, node)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeIdentity < nodes[j].NodeIdentity })
	return nodes, nil
}

func (r *NodeRegistry) DrainNode(ctx context.Context, nodeIdentity string) error {
	r.mu.Lock()
	node, ok := r.nodes[nodeIdentity]
	if !ok {
		r.mu.Unlock()
		return sp.ErrNodeNotFound
	}
	if _, invalid := r.invalid[groupKey(node.NodeType, node.NodeGroup)][node.NodeName]; !invalid {
		r.mu.Unlock()
		return sp.ErrNodeNotInvalid
	}
	if node.Status == sp.NodeStatusOffline {
		r.mu.Unlock()
		return sp.ErrNodeNotFound
	}
	node.Status = sp.NodeStatusDraining
	r.nodes[nodeIdentity] = node
	event := r.nodeEvent(sp.EventNodeDraining, node, r.now())
	r.mu.Unlock()
	return r.publish(ctx, event)
}

func (r *NodeRegistry) CompleteDrain(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	var event sp.PlacementEvent
	var err error
	if r.completeDrain != nil {
		event, err = r.completeDrain(nodeIdentity, nodeSessionID)
	} else {
		event, err = r.unregisterNode(nodeIdentity, nodeSessionID, nil)
	}
	if err != nil {
		return err
	}
	return r.publish(ctx, event)
}

func (r *NodeRegistry) MarkNodeInvalid(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error {
	identity, err := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	if err != nil {
		return err
	}
	r.mu.Lock()
	key := groupKey(nodeType, nodeGroup)
	if r.invalid[key] == nil {
		r.invalid[key] = make(map[string]struct{})
	}
	r.invalid[key][nodeName] = struct{}{}
	event := sp.PlacementEvent{Type: sp.EventNodeMarkedInvalid, NodeIdentity: identity.String(), NodeType: nodeType, NodeGroup: nodeGroup, NodeName: nodeName, Time: r.now()}
	r.mu.Unlock()
	return r.publish(ctx, event)
}

func (r *NodeRegistry) RestoreNode(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error {
	identity, err := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	if err != nil {
		return err
	}
	r.mu.Lock()
	key := groupKey(nodeType, nodeGroup)
	delete(r.invalid[key], nodeName)
	if len(r.invalid[key]) == 0 {
		delete(r.invalid, key)
	}
	event := sp.PlacementEvent{Type: sp.EventNodeRestored, NodeIdentity: identity.String(), NodeType: nodeType, NodeGroup: nodeGroup, NodeName: nodeName, Time: r.now()}
	r.mu.Unlock()
	return r.publish(ctx, event)
}

func (r *NodeRegistry) ListInvalidNodes(_ context.Context, nodeType string, nodeGroup string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var names []string
	for name := range r.invalid[groupKey(nodeType, nodeGroup)] {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (r *NodeRegistry) IsInvalid(nodeType string, nodeGroup string, nodeName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isInvalidLocked(nodeType, nodeGroup, nodeName)
}

func (r *NodeRegistry) Node(nodeIdentity string) (sp.Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.nodes[nodeIdentity]
	return node, ok
}

func (r *NodeRegistry) newLease(now time.Time) sp.NodeLease {
	return sp.NodeLease{Version: 1, TTLMillis: r.config.TTL.Milliseconds(), ExpireAtUnixMilli: now.Add(r.config.TTL).UnixMilli()}
}

func (r *NodeRegistry) nodeEvent(eventType sp.EventType, node sp.Node, now time.Time) sp.PlacementEvent {
	return sp.PlacementEvent{
		Type:             eventType,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		NodeType:         node.NodeType,
		NodeGroup:        node.NodeGroup,
		NodeName:         node.NodeName,
		NodeLeaseVersion: node.Lease.Version,
		Time:             now,
	}
}

func (r *NodeRegistry) isInvalidLocked(nodeType, nodeGroup, nodeName string) bool {
	_, ok := r.invalid[groupKey(nodeType, nodeGroup)][nodeName]
	return ok
}

func (r *NodeRegistry) publish(ctx context.Context, event sp.PlacementEvent) error {
	if r.publisher == nil {
		return nil
	}
	return r.publisher.Publish(ctx, event)
}

func normalizeNodeIdentity(node *sp.Node) error {
	if node.NodeSessionID == "" {
		return fmt.Errorf("node session ID is empty")
	}
	identity, err := sp.NewNodeIdentity(node.NodeType, node.NodeGroup, node.NodeName)
	if err != nil {
		return err
	}
	expected := identity.String()
	if node.NodeIdentity != expected {
		return fmt.Errorf("node identity mismatch: expected %q, got %q", expected, node.NodeIdentity)
	}
	return nil
}

func leaseExpired(node sp.Node, now time.Time) bool {
	return now.UnixMilli() >= node.Lease.ExpireAtUnixMilli
}

func leaseGrant(node sp.Node, now time.Time) sp.NodeLeaseGrant {
	return sp.NodeLeaseGrant{
		Version:    node.Lease.Version,
		ValidUntil: now.Add(time.UnixMilli(node.Lease.ExpireAtUnixMilli).Sub(now)),
	}
}

func groupKey(nodeType string, nodeGroup string) string { return nodeType + "/" + nodeGroup }
