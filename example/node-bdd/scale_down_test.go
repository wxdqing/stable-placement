//go:build integration

package nodebdd_test

import (
	"fmt"
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

// C. 缩容（Scale Down）

func TestScaleDown_C1_MarkInvalidBlocksNewAllocate(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("C1 MarkNodeInvalid 后新 Allocate 不选该 NodeName")

	node1 := h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")

	h.step("Given MarkNodeInvalid game-1")
	h.must(h.dir.MarkNodeInvalid(h.ctx, h.nodeType, h.nodeGroup, "game-1"), "MarkNodeInvalid")

	placement := h.allocate(h.grainID("c1"))
	if placement.NodeIdentity == node1.NodeIdentity || placement.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("allocated = %q, want %q", placement.NodeIdentity, node2.NodeIdentity)
	}
}

func TestScaleDown_C2_InvalidDoesNotChangeExistingLookup(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("C2 MarkNodeInvalid 后已有 Placement Lookup 不变")

	node1 := h.registerGame("game-1", "session-a")
	h.registerGame("game-2", "session-b")

	placement := h.allocate(h.grainID("c2"))
	h.must(h.dir.MarkNodeInvalid(h.ctx, h.nodeType, h.nodeGroup, "game-1"), "MarkNodeInvalid")

	found := h.lookup(placement.GrainKey)
	if found.NodeIdentity != node1.NodeIdentity {
		t.Fatalf("lookup changed: %q", found.NodeIdentity)
	}
}

func TestScaleDown_C3_InvalidSurvivesSessionReplacement(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("C3 InvalidNodeGroup 跨 NodeSessionID 持续")

	h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")

	h.must(h.dir.MarkNodeInvalid(h.ctx, h.nodeType, h.nodeGroup, "game-1"), "MarkNodeInvalid")
	h.replaceSession("game-1", "session-new")

	placement := h.allocate(h.grainID("c3"))
	if placement.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("invalid did not survive session replace: %q", placement.NodeIdentity)
	}
}

func TestScaleDown_C4_DrainRequiresMarkInvalid(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("C4 DrainNode 前未 MarkNodeInvalid 必须失败")

	node := h.registerGame("game-1", "session-a")
	err := h.dir.DrainNode(h.ctx, node.NodeIdentity)
	h.mustErrIs(err, sp.ErrNodeNotInvalid, "DrainNode before invalid")
}

func TestScaleDown_C5_DrainingNodeExcludedFromAllocate(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("C5 DrainNode 后节点 draining，不参与 Allocate")

	node1 := h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")

	h.must(h.dir.MarkNodeInvalid(h.ctx, h.nodeType, h.nodeGroup, "game-1"), "MarkNodeInvalid")
	h.must(h.dir.DrainNode(h.ctx, node1.NodeIdentity), "DrainNode")

	nodes := h.listGameNodes()
	for _, n := range nodes {
		if n.NodeIdentity == node1.NodeIdentity && n.Status != sp.NodeStatusDraining {
			t.Fatalf("game-1 status = %s, want draining", n.Status)
		}
	}

	placement := h.allocate(h.grainID("c5"))
	if placement.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("allocated = %q, want %q", placement.NodeIdentity, node2.NodeIdentity)
	}
}

func TestScaleDown_C6_FindByNodeListsPendingMigrations(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("C6 FindByNode 列出待迁移 Placement")

	node1 := h.registerGame("game-1", "session-a")
	h.allocate(h.grainID("c6-1"))
	h.allocate(h.grainID("c6-2"))
	h.allocate(h.grainID("c6-3"))
	h.registerGame("game-2", "session-b")

	first, err := h.dir.FindByNode(h.ctx, sp.FindByNodeQuery{
		NodeIdentity: node1.NodeIdentity,
		Limit:        1,
	})
	h.must(err, "FindByNode first page")
	if len(first.Placements) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %+v, want one placement and next cursor", first)
	}

	seen := map[sp.GrainKey]struct{}{first.Placements[0].GrainKey: {}}
	cursor := first.NextCursor
	for cursor != "" {
		page, err := h.dir.FindByNode(h.ctx, sp.FindByNodeQuery{
			NodeIdentity: node1.NodeIdentity,
			Limit:        1,
			Cursor:       cursor,
		})
		h.must(err, "FindByNode next page")
		for _, placement := range page.Placements {
			if _, duplicate := seen[placement.GrainKey]; duplicate {
				t.Fatalf("duplicate placement across pages: %s", placement.GrainKey)
			}
			seen[placement.GrainKey] = struct{}{}
		}
		cursor = page.NextCursor
	}
	if len(seen) != 3 {
		t.Fatalf("paginated placements = %d, want 3", len(seen))
	}
}

func TestScaleDown_C7_C8_FullShrinkWorkflow(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("C7/C8 缩容全流程：Transfer 迁走 → FindByNode 为空 → Unregister")

	node1 := h.registerGame("game-1", "session-a")
	h.allocate(h.grainID("c7-1"))
	h.allocate(h.grainID("c7-2"))
	node2 := h.registerGame("game-2", "session-b")

	h.step("1. MarkNodeInvalid game-1")
	h.must(h.dir.MarkNodeInvalid(h.ctx, h.nodeType, h.nodeGroup, "game-1"), "MarkNodeInvalid")

	h.step("2. DrainNode game-1")
	h.must(h.dir.DrainNode(h.ctx, node1.NodeIdentity), "DrainNode")

	h.step("3. FindByNode 有待迁移 Placement")
	if len(h.placementsOn(node1)) == 0 {
		t.Fatal("expected placements before transfer")
	}

	h.step("4. Transfer 全部到 game-2")
	h.transferAll(node1, node2)

	h.step("5. FindByNode game-1 为空")
	if len(h.placementsOn(node1)) != 0 {
		t.Fatalf("game-1 still has placements: %+v", h.placementsOn(node1))
	}

	h.step("6. UnregisterNode game-1")
	h.must(h.dir.CompleteDrain(h.ctx, node1.NodeIdentity, node1.NodeSessionID), "CompleteDrain")

	nodes := h.listGameNodes()
	if len(nodes) != 1 || nodes[0].NodeName != "game-2" {
		t.Fatalf("nodes after shrink = %+v", nodes)
	}
}

func TestScaleDown_C9_RestoreNodeRejoinsPool(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("C9 RestoreNode 后节点重新参与 Allocate")

	node1 := h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")

	h.must(h.dir.MarkNodeInvalid(h.ctx, h.nodeType, h.nodeGroup, "game-1"), "MarkNodeInvalid")
	p1 := h.allocate(h.grainID("c9-1"))
	if p1.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("before restore allocated = %q", p1.NodeIdentity)
	}

	h.step("When RestoreNode game-1")
	h.must(h.dir.RestoreNode(h.ctx, h.nodeType, h.nodeGroup, "game-1"), "RestoreNode")

	invalid, err := h.dir.ListInvalidNodes(h.ctx, h.nodeType, h.nodeGroup)
	h.must(err, "ListInvalidNodes")
	if len(invalid) != 0 {
		t.Fatalf("invalid nodes = %v", invalid)
	}

	// 恢复后 game-1 可再次被选中（RoundRobin 轮询）
	found := false
	for i := 0; i < 4; i++ {
		p := h.allocate(h.grainID(fmt.Sprintf("c9-restore-%d", i)))
		if p.NodeIdentity == node1.NodeIdentity {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("restored game-1 did not receive new allocation within 4 attempts")
	}
}
