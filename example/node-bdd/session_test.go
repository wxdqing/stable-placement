//go:build integration

package nodebdd_test

import (
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

// E. Session 与节点替换

func TestSession_E1_OldSessionRenewFailsAfterReplace(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("E1 ReplaceNodeSession 后旧 session Renew 失败")

	node := h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("e1"))

	h.replaceSession("game-1", "session-b")

	_, err := h.dir.Renew(h.ctx, sp.RenewCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: node.NodeIdentity, NodeSessionID: "session-a", PlacementVersion: placement.Version})
	h.mustErrIs(err, sp.ErrInvalidNodeSession, "Renew old session")
}

func TestSession_E2_NewSessionDoesNotInheritOldPlacement(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("E2 ReplaceNodeSession 后新 session 不继承旧 Placement")

	node := h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("e2"))

	h.replaceSession("game-1", "session-b")

	err := h.dir.Release(h.ctx, sp.ReleaseCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: node.NodeIdentity, NodeSessionID: "session-a", PlacementVersion: placement.Version})
	h.mustErrIs(err, sp.ErrInvalidNodeSession, "Release old session")

	_, err = h.dir.Lookup(h.ctx, placement.GrainKey)
	h.mustErrIs(err, sp.ErrPlacementNotFound, "Lookup after session replace")
	exists, err := h.dir.Exists(h.ctx, placement.GrainKey)
	h.must(err, "Exists after session replace")
	if exists {
		t.Fatal("new session inherited old placement")
	}
	retained := h.placementsOn(node)
	if len(retained) != 1 || retained[0].OwnerNodeSessionID != "session-a" || retained[0].Version != placement.Version {
		t.Fatalf("retained placement = %+v, want old session placement", retained)
	}
}

func TestSession_E3_UnregisterWrongSessionFails(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("E3 UnregisterNode 错误 session 失败")

	node := h.registerGame("game-1", "session-a")
	err := h.dir.UnregisterNode(h.ctx, node.NodeIdentity, "wrong-session")
	h.mustErrIs(err, sp.ErrInvalidNodeSession, "Unregister wrong session")

	nodes := h.listGameNodes()
	if len(nodes) != 1 {
		t.Fatalf("node unregistered unexpectedly: %+v", nodes)
	}
}
