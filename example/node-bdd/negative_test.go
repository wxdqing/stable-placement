//go:build integration

package nodebdd_test

import (
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

// F. 边界与负向

func TestNegative_F1_AllocateWithoutNodesFails(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("F1 无节点时 Allocate 失败")

	_, err := h.dir.Allocate(h.ctx, sp.AllocateCommand{
		GrainID:         h.grainID("f1"),
		Kind:            "Player",
		TargetNodeType:  h.nodeType,
		TargetNodeGroup: h.nodeGroup,
		LeaseTTL:        time.Minute,
	})
	h.mustErrIs(err, sp.ErrNoAvailableNode, "Allocate without nodes")
}

func TestNegative_F2_AllNodesInvalidAllocateFails(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("F2 全部节点 Invalid 时 Allocate 失败")

	h.registerGame("game-1", "session-a")
	h.must(h.dir.MarkNodeInvalid(h.ctx, h.nodeType, h.nodeGroup, "game-1"), "MarkNodeInvalid")

	_, err := h.dir.Allocate(h.ctx, sp.AllocateCommand{
		GrainID:         h.grainID("f2"),
		Kind:            "Player",
		TargetNodeType:  h.nodeType,
		TargetNodeGroup: h.nodeGroup,
		LeaseTTL:        time.Minute,
	})
	h.mustErrIs(err, sp.ErrNoAvailableNode, "Allocate all invalid")
}

func TestNegative_F3_TransferToDrainingNodeFails(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("F3 Transfer 到 draining 节点失败")

	node1 := h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")
	placement := h.allocate(h.grainID("f3"))

	h.must(h.dir.MarkNodeInvalid(h.ctx, h.nodeType, h.nodeGroup, "game-2"), "MarkNodeInvalid")
	h.must(h.dir.DrainNode(h.ctx, node2.NodeIdentity), "DrainNode")

	_, err := h.dir.Transfer(h.ctx, sp.TransferCommand{
		GrainKey:         placement.GrainKey,
		FromNodeIdentity: node1.NodeIdentity,
		ToNodeIdentity:   node2.NodeIdentity,
		PlacementVersion: placement.Version,
		LeaseTTL:         time.Minute,
	})
	h.mustErrIs(err, sp.ErrNoAvailableNode, "Transfer to draining")
}

func TestNegative_F4_VersionConflictOnRenewAndRelease(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("F4 Version 冲突时 Renew 和 Release 失败")

	node := h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("f4"))

	_, err := h.dir.Renew(h.ctx, sp.RenewCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: placement.Version + 99,
		LeaseVersion:     placement.Lease.Version,
		ExtendTTL:        time.Minute,
	})
	h.mustErrIs(err, sp.ErrVersionConflict, "Renew version conflict")

	err = h.dir.Release(h.ctx, sp.ReleaseCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version + 99,
	})
	h.mustErrIs(err, sp.ErrVersionConflict, "Release version conflict")

	found := h.lookup(placement.GrainKey)
	if found.Status != sp.PlacementStatusActive {
		t.Fatalf("version-conflicted release changed placement: %+v", found)
	}
}
