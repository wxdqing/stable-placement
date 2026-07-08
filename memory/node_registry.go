package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

type NodeRegistry struct {
	mu           sync.RWMutex
	nodes        map[string]sp.Node
	invalid      map[string]map[string]struct{}
	publisher    sp.EventPublisher
	heartbeatTTL time.Duration
}

func NewNodeRegistry(publisher sp.EventPublisher) *NodeRegistry {
	return &NodeRegistry{
		nodes:     make(map[string]sp.Node),
		invalid:   make(map[string]map[string]struct{}),
		publisher: publisher,
	}
}

func (r *NodeRegistry) SetHeartbeatTTL(ttl time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heartbeatTTL = ttl
}

func (r *NodeRegistry) RegisterNode(ctx context.Context, node sp.Node) error {
	r.mu.Lock()
	r.normalizeNode(&node)
	r.nodes[node.NodeIdentity] = node
	event := sp.PlacementEvent{
		Type:         sp.EventNodeRegistered,
		NodeIdentity: node.NodeIdentity,
		NodeType:     node.NodeType,
		NodeGroup:    node.NodeGroup,
		NodeName:     node.NodeName,
		Time:         time.Now(),
	}
	r.mu.Unlock()
	return r.publish(ctx, event)
}

func (r *NodeRegistry) RenewNode(_ context.Context, nodeIdentity string, nodeSessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodes[nodeIdentity]
	if !ok {
		return sp.ErrNodeNotFound
	}
	if node.NodeSessionID != nodeSessionID {
		return sp.ErrInvalidNodeSession
	}
	node.LastHeartbeatAt = time.Now()
	r.nodes[nodeIdentity] = node
	return nil
}

func (r *NodeRegistry) UnregisterNode(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	r.mu.Lock()
	node, ok := r.nodes[nodeIdentity]
	if !ok {
		r.mu.Unlock()
		return sp.ErrNodeNotFound
	}
	if node.NodeSessionID != nodeSessionID {
		r.mu.Unlock()
		return sp.ErrInvalidNodeSession
	}
	delete(r.nodes, nodeIdentity)
	event := sp.PlacementEvent{
		Type:         sp.EventNodeUnregistered,
		NodeIdentity: nodeIdentity,
		NodeType:     node.NodeType,
		NodeGroup:    node.NodeGroup,
		NodeName:     node.NodeName,
		Time:         time.Now(),
	}
	r.mu.Unlock()
	return r.publish(ctx, event)
}

func (r *NodeRegistry) ReplaceNodeSession(ctx context.Context, node sp.Node) (*sp.Node, error) {
	r.mu.Lock()
	r.normalizeNode(&node)
	old, _ := r.nodes[node.NodeIdentity]
	r.nodes[node.NodeIdentity] = node
	event := sp.PlacementEvent{
		Type:         sp.EventNodeReplaced,
		NodeIdentity: node.NodeIdentity,
		NodeType:     node.NodeType,
		NodeGroup:    node.NodeGroup,
		NodeName:     node.NodeName,
		Time:         time.Now(),
	}
	r.mu.Unlock()
	return &old, r.publish(ctx, event)
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
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeIdentity < nodes[j].NodeIdentity
	})
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
	node.Status = sp.NodeStatusDraining
	r.nodes[nodeIdentity] = node
	event := sp.PlacementEvent{
		Type:         sp.EventNodeDraining,
		NodeIdentity: nodeIdentity,
		NodeType:     node.NodeType,
		NodeGroup:    node.NodeGroup,
		NodeName:     node.NodeName,
		Time:         time.Now(),
	}
	r.mu.Unlock()
	return r.publish(ctx, event)
}

func (r *NodeRegistry) CompleteDrain(ctx context.Context, nodeIdentity string, nodeSessionID string) error {
	return r.UnregisterNode(ctx, nodeIdentity, nodeSessionID)
}

func (r *NodeRegistry) ExpireHeartbeats(ctx context.Context, now time.Time) error {
	r.mu.Lock()
	if r.heartbeatTTL <= 0 {
		r.mu.Unlock()
		return nil
	}
	var events []sp.PlacementEvent
	for identity, node := range r.nodes {
		if node.Status == sp.NodeStatusOffline {
			continue
		}
		if now.Sub(node.LastHeartbeatAt) <= r.heartbeatTTL {
			continue
		}
		node.Status = sp.NodeStatusOffline
		r.nodes[identity] = node
		events = append(events, sp.PlacementEvent{
			Type:         sp.EventNodeUnregistered,
			NodeIdentity: identity,
			NodeType:     node.NodeType,
			NodeGroup:    node.NodeGroup,
			NodeName:     node.NodeName,
			Time:         now,
		})
	}
	r.mu.Unlock()
	for _, event := range events {
		if err := r.publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (r *NodeRegistry) MarkNodeInvalid(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error {
	r.mu.Lock()
	key := groupKey(nodeType, nodeGroup)
	if r.invalid[key] == nil {
		r.invalid[key] = make(map[string]struct{})
	}
	r.invalid[key][nodeName] = struct{}{}
	identity, _ := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	event := sp.PlacementEvent{
		Type:         sp.EventNodeMarkedInvalid,
		NodeIdentity: identity.String(),
		NodeType:     nodeType,
		NodeGroup:    nodeGroup,
		NodeName:     nodeName,
		Time:         time.Now(),
	}
	r.mu.Unlock()
	return r.publish(ctx, event)
}

func (r *NodeRegistry) RestoreNode(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error {
	r.mu.Lock()
	key := groupKey(nodeType, nodeGroup)
	delete(r.invalid[key], nodeName)
	if len(r.invalid[key]) == 0 {
		delete(r.invalid, key)
	}
	identity, _ := sp.NewNodeIdentity(nodeType, nodeGroup, nodeName)
	event := sp.PlacementEvent{
		Type:         sp.EventNodeRestored,
		NodeIdentity: identity.String(),
		NodeType:     nodeType,
		NodeGroup:    nodeGroup,
		NodeName:     nodeName,
		Time:         time.Now(),
	}
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
	_, ok := r.invalid[groupKey(nodeType, nodeGroup)][nodeName]
	return ok
}

func (r *NodeRegistry) SessionMatches(nodeIdentity string, nodeSessionID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.nodes[nodeIdentity]
	return ok && node.NodeSessionID == nodeSessionID
}

func (r *NodeRegistry) Node(nodeIdentity string) (sp.Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.nodes[nodeIdentity]
	return node, ok
}

func (r *NodeRegistry) normalizeNode(node *sp.Node) {
	if node.NodeIdentity == "" {
		identity, _ := sp.NewNodeIdentity(node.NodeType, node.NodeGroup, node.NodeName)
		node.NodeIdentity = identity.String()
	}
	if node.Status == "" {
		node.Status = sp.NodeStatusActive
	}
	if node.LastHeartbeatAt.IsZero() {
		node.LastHeartbeatAt = time.Now()
	}
}

func (r *NodeRegistry) publish(ctx context.Context, event sp.PlacementEvent) error {
	if r.publisher == nil {
		return nil
	}
	return r.publisher.Publish(ctx, event)
}

func groupKey(nodeType string, nodeGroup string) string {
	return nodeType + "/" + nodeGroup
}
