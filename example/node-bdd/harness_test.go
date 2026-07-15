//go:build integration

package nodebdd_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/redis"
)

const defaultRedisAddr = "127.0.0.1:6379"

type harness struct {
	t      *testing.T
	ctx    context.Context
	client *goredis.Client
	dir    *redis.Directory
	runID  string

	nodeType  string
	nodeGroup string

	nodes map[string]sp.Node
}

func newHarness(t *testing.T) *harness {
	return newHarnessWithNodeLeaseConfig(t, sp.DefaultNodeLeaseConfig())
}

func newHarnessWithNodeLeaseConfig(t *testing.T, config sp.NodeLeaseConfig) *harness {
	t.Helper()
	addr := os.Getenv("STABLE_PLACEMENT_REDIS_ADDR")
	if addr == "" {
		addr = defaultRedisAddr
	}
	client := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: os.Getenv("STABLE_PLACEMENT_REDIS_PASSWORD"),
	})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis %s unavailable: %v", addr, err)
	}
	dir, err := redis.NewDirectory(client, sp.StrategyModeRedisRoundRobin, config)
	if err != nil {
		t.Fatalf("NewDirectory error: %v", err)
	}
	runID := fmt.Sprintf("%x", time.Now().UnixNano())
	return &harness{
		t:         t,
		ctx:       ctx,
		client:    client,
		dir:       dir,
		runID:     runID,
		nodeType:  "game",
		nodeGroup: "nbdd-" + runID,
		nodes:     make(map[string]sp.Node),
	}
}

func (h *harness) cleanup() {
	seen := make(map[string]struct{}, len(h.nodes))
	for _, node := range h.nodes {
		if _, ok := seen[node.NodeIdentity]; ok {
			continue
		}
		seen[node.NodeIdentity] = struct{}{}
		for _, placement := range h.placementsOn(node) {
			if err := h.releasePlacement(placement); err != nil {
				h.t.Errorf("cleanup Release %s failed: %v", placement.GrainKey, err)
			}
		}
	}
	for _, node := range h.nodes {
		if err := h.dir.UnregisterNode(h.ctx, node.NodeIdentity, node.NodeSessionID); err != nil &&
			!errors.Is(err, sp.ErrNodeNotFound) {
			h.t.Errorf("cleanup UnregisterNode %s failed: %v", node.NodeIdentity, err)
		}
		if err := h.dir.RestoreNode(h.ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
			h.t.Errorf("cleanup RestoreNode %s failed: %v", node.NodeIdentity, err)
		}
	}
	if err := h.client.Close(); err != nil {
		h.t.Errorf("cleanup Redis client close failed: %v", err)
	}
}

func (h *harness) releasePlacement(placement sp.Placement) error {
	err := h.dir.Release(h.ctx, sp.ReleaseCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: placement.NodeIdentity, NodeSessionID: placement.OwnerNodeSessionID, PlacementVersion: placement.Version})
	if err == nil {
		return nil
	}
	for _, node := range h.nodes {
		recovered, recoverErr := h.dir.Recover(h.ctx, sp.RecoverCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NewNodeIdentity: node.NodeIdentity, PlacementVersion: placement.Version})
		if recoverErr != nil {
			continue
		}
		return h.dir.Release(h.ctx, sp.ReleaseCommand{GrainKey: recovered.GrainKey, PlacementID: recovered.PlacementID, NodeIdentity: recovered.NodeIdentity, NodeSessionID: recovered.OwnerNodeSessionID, PlacementVersion: recovered.Version})
	}
	return err
}

func (h *harness) scenario(name string) {
	h.t.Helper()
	h.t.Logf("\n## %s", name)
}

func (h *harness) step(format string, args ...any) {
	h.t.Helper()
	h.t.Logf("  -> "+format, args...)
}

func (h *harness) must(err error, msg string) {
	h.t.Helper()
	if err != nil {
		h.t.Fatalf("%s: %v", msg, err)
	}
}

func (h *harness) mustErrIs(err error, target error, msg string) {
	h.t.Helper()
	if !errors.Is(err, target) {
		h.t.Fatalf("%s: err=%v want=%v", msg, err, target)
	}
}

func (h *harness) registerGame(name string, session string) sp.Node {
	h.t.Helper()
	identity, err := sp.NewNodeIdentity(h.nodeType, h.nodeGroup, name)
	h.must(err, "NewNodeIdentity")
	node := sp.Node{
		NodeType:      h.nodeType,
		NodeGroup:     h.nodeGroup,
		NodeName:      name,
		NodeIdentity:  identity.String(),
		NodeSessionID: session,
		Status:        sp.NodeStatusActive,
	}
	_, err = h.dir.RegisterNode(h.ctx, node)
	h.must(err, "RegisterNode "+name)
	h.nodes[name] = node
	return node
}

func (h *harness) replaceSession(name string, newSession string) sp.Node {
	h.t.Helper()
	node := h.nodes[name]
	node.NodeSessionID = newSession
	old, _, err := h.dir.ReplaceNodeSession(h.ctx, node)
	h.must(err, "ReplaceNodeSession "+name)
	h.nodes[name] = node
	if old != nil {
		h.t.Logf("  old session = %s", old.NodeSessionID)
	}
	return node
}

func (h *harness) grainID(suffix string) string {
	return fmt.Sprintf("%s-%s", h.runID, suffix)
}

func (h *harness) allocate(grainID string) *sp.Placement {
	h.t.Helper()
	placement, err := h.dir.Allocate(h.ctx, sp.AllocateCommand{
		GrainID:         grainID,
		Kind:            "Player",
		TargetNodeType:  h.nodeType,
		TargetNodeGroup: h.nodeGroup,
	})
	h.must(err, "Allocate "+grainID)
	return placement
}

func (h *harness) lookup(key sp.GrainKey) *sp.PlacementRoute {
	h.t.Helper()
	placement, err := h.dir.Lookup(h.ctx, key)
	h.must(err, "Lookup "+key.String())
	return placement
}

func (h *harness) waitForLookupError(key sp.GrainKey, target error) {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, err := h.dir.Lookup(h.ctx, key)
		if errors.Is(err, target) {
			return
		}
		if err != nil {
			h.t.Fatalf("Lookup %s: err=%v want=%v", key, err, target)
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("Lookup %s did not return %v before deadline", key, target)
}

func (h *harness) listGameNodes() []sp.Node {
	h.t.Helper()
	nodes, err := h.dir.FindNodes(h.ctx, h.nodeType, h.nodeGroup)
	h.must(err, "FindNodes")
	return nodes
}

func (h *harness) placementsOn(node sp.Node) []sp.Placement {
	h.t.Helper()
	var all []sp.Placement
	cursor := ""
	for {
		page, err := h.dir.FindByNode(h.ctx, sp.FindByNodeQuery{
			NodeIdentity: node.NodeIdentity,
			Limit:        50,
			Cursor:       cursor,
		})
		h.must(err, "FindByNode")
		all = append(all, page.Placements...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return all
}

func (h *harness) nodeIdentities(nodes []sp.Node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.NodeIdentity
	}
	sort.Strings(ids)
	return ids
}

func (h *harness) assertSortedNodeList(wantNames []string) {
	h.t.Helper()
	nodes := h.listGameNodes()
	if len(nodes) != len(wantNames) {
		h.t.Fatalf("node count = %d, want %d: %+v", len(nodes), len(wantNames), nodes)
	}
	for i, want := range wantNames {
		if nodes[i].NodeName != want {
			h.t.Fatalf("nodes[%d].NodeName = %q, want %q", i, nodes[i].NodeName, want)
		}
	}
	ids := h.nodeIdentities(nodes)
	for i := 1; i < len(ids); i++ {
		if ids[i] < ids[i-1] {
			h.t.Fatalf("nodes not sorted: %v", ids)
		}
	}
}

func (h *harness) transferAll(from sp.Node, to sp.Node) {
	h.t.Helper()
	for {
		page, err := h.dir.FindByNode(h.ctx, sp.FindByNodeQuery{
			NodeIdentity: from.NodeIdentity,
			Limit:        10,
		})
		h.must(err, "FindByNode transfer")
		if len(page.Placements) == 0 {
			return
		}
		for _, p := range page.Placements {
			_, err := h.dir.Transfer(h.ctx, sp.TransferCommand{GrainKey: p.GrainKey, PlacementID: p.PlacementID, FromNodeIdentity: from.NodeIdentity, ToNodeIdentity: to.NodeIdentity, PlacementVersion: p.Version})
			h.must(err, "Transfer "+p.GrainKey.String())
		}
	}
}
