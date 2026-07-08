package memory

import (
	"strings"
	"sync"

	sp "github.com/wxdqing/stable-placement"
)

type PlacementCache struct {
	mu       sync.RWMutex
	entries  map[sp.GrainKey]sp.PlacementRoute
	byNode   map[string]map[sp.GrainKey]struct{}
	degraded bool
}

func NewPlacementCache() *PlacementCache {
	return &PlacementCache{
		entries: make(map[sp.GrainKey]sp.PlacementRoute),
		byNode:  make(map[string]map[sp.GrainKey]struct{}),
	}
}

func (c *PlacementCache) GetCachedPlacement(key sp.GrainKey) (*sp.PlacementRoute, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.degraded {
		return nil, false
	}
	route, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	return &route, true
}

func (c *PlacementCache) SetCachedPlacement(key sp.GrainKey, route sp.PlacementRoute) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.degraded {
		return
	}
	if old, ok := c.entries[key]; ok {
		c.deleteNodeIndexLocked(key, old.NodeIdentity)
	}
	c.entries[key] = route
	if c.byNode[route.NodeIdentity] == nil {
		c.byNode[route.NodeIdentity] = make(map[sp.GrainKey]struct{})
	}
	c.byNode[route.NodeIdentity][key] = struct{}{}
}

func (c *PlacementCache) DeleteCachedPlacement(key sp.GrainKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.entries[key]; ok {
		delete(c.entries, key)
		c.deleteNodeIndexLocked(key, old.NodeIdentity)
	}
}

func (c *PlacementCache) DeleteCachedPlacementsByNode(nodeIdentity string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.byNode[nodeIdentity] {
		delete(c.entries, key)
	}
	delete(c.byNode, nodeIdentity)
}

func (c *PlacementCache) DeleteCachedPlacementsByGroup(nodeType string, nodeGroup string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := nodeType + "/" + nodeGroup + "/"
	for nodeIdentity, keys := range c.byNode {
		if !strings.HasPrefix(nodeIdentity, prefix) {
			continue
		}
		for key := range keys {
			delete(c.entries, key)
		}
		delete(c.byNode, nodeIdentity)
	}
}

func (c *PlacementCache) ClearPlacementCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[sp.GrainKey]sp.PlacementRoute)
	c.byNode = make(map[string]map[sp.GrainKey]struct{})
}

func (c *PlacementCache) SetDegraded(degraded bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if degraded {
		c.entries = make(map[sp.GrainKey]sp.PlacementRoute)
		c.byNode = make(map[string]map[sp.GrainKey]struct{})
	}
	c.degraded = degraded
}

func (c *PlacementCache) IsDegraded() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.degraded
}

func (c *PlacementCache) deleteNodeIndexLocked(key sp.GrainKey, nodeIdentity string) {
	keys := c.byNode[nodeIdentity]
	if keys == nil {
		return
	}
	delete(keys, key)
	if len(keys) == 0 {
		delete(c.byNode, nodeIdentity)
	}
}
