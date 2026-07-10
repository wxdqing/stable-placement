//go:build integration

package nodebdd_test

import (
	"sync"
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

// B. 扩容（Scale Up）

func TestScaleUp_B1_SingleNodeAllocate(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("B1 单节点 Allocate 归属该节点")

	node := h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("b1"))

	if placement.NodeIdentity != node.NodeIdentity {
		t.Fatalf("owner = %q, want %q", placement.NodeIdentity, node.NodeIdentity)
	}
}

func TestScaleUp_B2_AddNodesVisibleInCluster(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("B2 扩容后 FindNodes 可见新节点")

	h.registerGame("game-1", "session-a")
	h.step("When 扩容 game-2, game-3")
	h.registerGame("game-2", "session-b")
	h.registerGame("game-3", "session-c")

	h.assertSortedNodeList([]string{"game-1", "game-2", "game-3"})
}

func TestScaleUp_B3_NewAllocateUsesExpandedPool(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("B3 扩容后新 Allocate 使用 RoundRobin 选到新节点")

	h.registerGame("game-1", "session-a")
	h.registerGame("game-2", "session-b")

	p1 := h.allocate(h.grainID("b3-1"))
	p2 := h.allocate(h.grainID("b3-2"))

	h.step("When 扩容 game-3")
	node3 := h.registerGame("game-3", "session-c")
	p3 := h.allocate(h.grainID("b3-3"))

	if p1.NodeIdentity == p2.NodeIdentity {
		t.Fatalf("pre-scale round robin did not use distinct nodes: %q", p1.NodeIdentity)
	}
	if p3.NodeIdentity != node3.NodeIdentity {
		t.Fatalf("post-scale allocation owner = %q, want newly added %q", p3.NodeIdentity, node3.NodeIdentity)
	}
}

func TestScaleUp_B4_ExistingPlacementStableAfterScaleUp(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("B4 扩容不自动迁移已有 Placement")

	node1 := h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("b4"))

	h.step("When 扩容 game-2")
	h.registerGame("game-2", "session-b")

	found := h.lookup(placement.GrainKey)
	if found.NodeIdentity != node1.NodeIdentity {
		t.Fatalf("placement moved after scale up: %q -> %q", node1.NodeIdentity, found.NodeIdentity)
	}
}

func TestScaleUp_B5_ConcurrentAllocateUniqueGrain(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("B5 并发 Allocate 同一 Grain 仅一个 Active")

	h.registerGame("game-1", "session-a")
	h.registerGame("game-2", "session-b")

	grainID := h.grainID("b5")
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := h.dir.Allocate(h.ctx, sp.AllocateCommand{
				GrainID:         grainID,
				Kind:            "Player",
				TargetNodeType:  h.nodeType,
				TargetNodeGroup: h.nodeGroup,
				LeaseTTL:        0,
			})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		h.must(err, "concurrent Allocate")
	}

	key, _ := sp.NewGrainKey("Player", grainID)
	ok, err := h.dir.Exists(h.ctx, key)
	h.must(err, "Exists")
	if !ok {
		t.Fatal("no active placement after concurrent allocate")
	}
}
