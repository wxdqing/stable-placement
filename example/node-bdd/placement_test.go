//go:build integration

package nodebdd_test

import (
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

// D. Placement 命令

func TestPlacement_D1_LookupNotFoundWithoutAllocate(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D1 Lookup 未分配返回 NotFound")

	h.registerGame("game-1", "session-a")
	key, _ := sp.NewGrainKey("Player", h.grainID("d1"))
	_, err := h.dir.Lookup(h.ctx, key)
	h.mustErrIs(err, sp.ErrPlacementNotFound, "Lookup before allocate")
	exists, err := h.dir.Exists(h.ctx, key)
	h.must(err, "Exists after Lookup")
	if exists {
		t.Fatal("Lookup created a placement")
	}
}

func TestPlacement_D2_LookupMatchesAllocate(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D2 Lookup 与 Allocate 一致")

	h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("d2"))
	found := h.lookup(placement.GrainKey)
	if found.NodeIdentity != placement.NodeIdentity || found.Version != placement.Version {
		t.Fatalf("lookup mismatch: got %+v want %+v", found, placement)
	}
}

func TestPlacement_D3_RenewValidatesWithoutChangingPlacementOrNodeLease(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D3 Renew 仅校验 Owner，不修改 Placement 或 Node Lease")

	h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("d3"))
	leaseBefore := h.listGameNodes()[0].Lease

	renewed, err := h.dir.Renew(h.ctx, sp.RenewCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: placement.NodeIdentity, NodeSessionID: placement.OwnerNodeSessionID, PlacementVersion: placement.Version})
	h.must(err, "Renew")
	if renewed.Version != placement.Version || renewed.NodeIdentity != placement.NodeIdentity || renewed.OwnerNodeSessionID != placement.OwnerNodeSessionID {
		t.Fatalf("renewed placement = %+v, want unchanged %+v", renewed, placement)
	}
	if leaseAfter := h.listGameNodes()[0].Lease; leaseAfter != leaseBefore {
		t.Fatalf("Renew changed node lease: before=%+v after=%+v", leaseBefore, leaseAfter)
	}
	persisted := h.placementsOn(sp.Node{NodeIdentity: placement.NodeIdentity})
	if len(persisted) != 1 {
		t.Fatalf("Renew changed persisted placement: got=%+v want=%+v", persisted, placement)
	}
	got := persisted[0]
	if got.GrainID != placement.GrainID || got.Kind != placement.Kind || got.GrainKey != placement.GrainKey || got.NodeIdentity != placement.NodeIdentity || got.OwnerNodeSessionID != placement.OwnerNodeSessionID || got.Version != placement.Version || got.Status != placement.Status || !got.CreateTime.Equal(placement.CreateTime) || !got.UpdateTime.Equal(placement.UpdateTime) {
		t.Fatalf("Renew changed persisted placement: got=%+v want=%+v", got, placement)
	}
}

func TestPlacement_D4_RenewRejectsWrongOwnerAndSession(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D4 非 Owner / 旧 session Renew 失败")

	node1 := h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")
	placement := h.allocate(h.grainID("d4"))

	_, err := h.dir.Renew(h.ctx, sp.RenewCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: node2.NodeIdentity, NodeSessionID: node2.NodeSessionID, PlacementVersion: placement.Version})
	h.mustErrIs(err, sp.ErrInvalidOwner, "Renew wrong owner")

	_, err = h.dir.Renew(h.ctx, sp.RenewCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: node1.NodeIdentity, NodeSessionID: "stale-session", PlacementVersion: placement.Version})
	h.mustErrIs(err, sp.ErrInvalidNodeSession, "Renew stale session")
}

func TestPlacement_D5_ReleaseThenReallocate(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D5 Release 后可重新 Allocate")

	h.registerGame("game-1", "session-a")
	grainID := h.grainID("d5")
	placement := h.allocate(grainID)

	h.must(h.dir.Release(h.ctx, sp.ReleaseCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: placement.NodeIdentity, NodeSessionID: placement.OwnerNodeSessionID, PlacementVersion: placement.Version}), "Release")

	_, err := h.dir.Lookup(h.ctx, placement.GrainKey)
	h.mustErrIs(err, sp.ErrPlacementNotFound, "Lookup after release")

	reallocated := h.allocate(grainID)
	if reallocated.Status != sp.PlacementStatusActive {
		t.Fatalf("status = %s", reallocated.Status)
	}
}

func TestPlacement_D6_RecoverRejectsHealthyOwner(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D6 健康 Owner 必须使用 Transfer，Recover 返回 NotRecoverable")

	node1 := h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")
	placement := h.allocate(h.grainID("d6"))
	target := node2
	if placement.NodeIdentity == node2.NodeIdentity {
		target = node1
	}

	_, err := h.dir.Recover(h.ctx, sp.RecoverCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NewNodeIdentity: target.NodeIdentity, PlacementVersion: placement.Version})
	h.mustErrIs(err, sp.ErrPlacementNotRecoverable, "Recover healthy owner")
	retained := h.placementsOn(sp.Node{NodeIdentity: placement.NodeIdentity})
	if len(retained) != 1 || retained[0].Version != placement.Version || retained[0].NodeIdentity != placement.NodeIdentity {
		t.Fatalf("healthy Recover changed placement: %+v", retained)
	}
}

func TestPlacement_D7_TransferChangesOwner(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D7 Transfer 显式更换 Owner")

	node1 := h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")
	placement := h.allocate(h.grainID("d7"))
	owner, target := node1, node2
	if placement.NodeIdentity == node2.NodeIdentity {
		owner, target = node2, node1
	}

	transferred, err := h.dir.Transfer(h.ctx, sp.TransferCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, FromNodeIdentity: placement.NodeIdentity, ToNodeIdentity: target.NodeIdentity, PlacementVersion: placement.Version})
	h.must(err, "Transfer")
	if transferred.NodeIdentity != target.NodeIdentity || transferred.OwnerNodeSessionID != target.NodeSessionID || transferred.Version != placement.Version+1 {
		t.Fatalf("transferred = %+v", transferred)
	}
	persisted := h.placementsOn(target)
	if len(persisted) != 1 || persisted[0].GrainKey != placement.GrainKey || persisted[0].NodeIdentity != target.NodeIdentity || persisted[0].OwnerNodeSessionID != target.NodeSessionID || persisted[0].Version != placement.Version+1 || persisted[0].Status != sp.PlacementStatusActive {
		t.Fatalf("target persisted placement = %+v", persisted)
	}
	if old := h.placementsOn(owner); len(old) != 0 {
		t.Fatalf("old owner placements after Transfer = %+v", old)
	}
}

func TestPlacement_D8_NodeLeaseExpiryInvalidatesAllRoutesButRetainsPlacements(t *testing.T) {
	h := newHarnessWithNodeLeaseConfig(t, sp.NodeLeaseConfig{TTL: 100 * time.Millisecond})
	defer h.cleanup()
	h.scenario("D8 Node Lease 到期后同节点所有 Grain 逻辑失效，但 Placement 保留")

	node := h.registerGame("game-1", "session-a")
	placements := []*sp.Placement{
		h.allocate(h.grainID("d8-1")),
		h.allocate(h.grainID("d8-2")),
	}
	h.waitForLookupError(placements[0].GrainKey, sp.ErrPlacementNotFound)

	want := make(map[sp.GrainKey]*sp.Placement, len(placements))
	for _, placement := range placements {
		want[placement.GrainKey] = placement
		_, err := h.dir.Lookup(h.ctx, placement.GrainKey)
		h.mustErrIs(err, sp.ErrPlacementNotFound, "Lookup after node lease expiry")
		exists, err := h.dir.Exists(h.ctx, placement.GrainKey)
		h.must(err, "Exists after node lease expiry")
		if exists {
			t.Fatalf("Exists(%s) = true after node lease expiry", placement.GrainKey)
		}
	}
	retained := h.placementsOn(node)
	if len(retained) != len(placements) {
		t.Fatalf("retained placements = %+v, want %d", retained, len(placements))
	}
	for _, placement := range retained {
		original := want[placement.GrainKey]
		if original == nil || placement.Status != sp.PlacementStatusActive || placement.Version != original.Version || placement.OwnerNodeSessionID != original.OwnerNodeSessionID {
			t.Fatalf("retained placement changed: %+v", placement)
		}
	}
}

func TestPlacement_D9_ExpiredSameSessionCanRelease(t *testing.T) {
	h := newHarnessWithNodeLeaseConfig(t, sp.NodeLeaseConfig{TTL: 100 * time.Millisecond})
	defer h.cleanup()
	h.scenario("D9 Node Lease 已到期时，同 session 仍可安全 Release")

	node := h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("d9"))
	h.waitForLookupError(placement.GrainKey, sp.ErrPlacementNotFound)

	retained := h.placementsOn(node)
	if len(retained) != 1 {
		t.Fatalf("FindByNode placements = %+v", retained)
	}
	h.must(h.dir.Release(h.ctx, sp.ReleaseCommand{GrainKey: retained[0].GrainKey, PlacementID: retained[0].PlacementID, NodeIdentity: retained[0].NodeIdentity, NodeSessionID: retained[0].OwnerNodeSessionID, PlacementVersion: retained[0].Version}), "Release expired same-session placement")
	if got := h.placementsOn(node); len(got) != 0 {
		t.Fatalf("placements after Release = %+v", got)
	}
}

func TestPlacement_D10_ExistsOnlyForActive(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D10 Exists 仅对 Active Placement 为 true")

	h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("d10"))

	ok, err := h.dir.Exists(h.ctx, placement.GrainKey)
	h.must(err, "Exists active")
	if !ok {
		t.Fatal("Exists = false for active placement")
	}

	h.must(h.dir.Release(h.ctx, sp.ReleaseCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: placement.NodeIdentity, NodeSessionID: placement.OwnerNodeSessionID, PlacementVersion: placement.Version}), "Release")

	ok, err = h.dir.Exists(h.ctx, placement.GrainKey)
	h.must(err, "Exists released")
	if ok {
		t.Fatal("Exists = true after release")
	}
}
