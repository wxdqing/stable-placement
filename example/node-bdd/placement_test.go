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

func TestPlacement_D3_RenewAdvancesLeaseVersion(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D3 Renew 推进 LeaseVersion")

	node := h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("d3"))

	renewed, err := h.dir.Renew(h.ctx, sp.RenewCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
		ExtendTTL:        time.Minute,
	})
	h.must(err, "Renew")
	if renewed.Lease.Version != placement.Lease.Version+1 {
		t.Fatalf("lease version = %d", renewed.Lease.Version)
	}
}

func TestPlacement_D4_RenewRejectsWrongOwnerAndSession(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D4 非 Owner / 旧 session Renew 失败")

	node1 := h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")
	placement := h.allocate(h.grainID("d4"))

	_, err := h.dir.Renew(h.ctx, sp.RenewCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node2.NodeIdentity,
		NodeSessionID:    node2.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
		ExtendTTL:        time.Minute,
	})
	h.mustErrIs(err, sp.ErrInvalidOwner, "Renew wrong owner")

	_, err = h.dir.Renew(h.ctx, sp.RenewCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node1.NodeIdentity,
		NodeSessionID:    "stale-session",
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
		ExtendTTL:        time.Minute,
	})
	h.mustErrIs(err, sp.ErrInvalidNodeSession, "Renew stale session")
}

func TestPlacement_D5_ReleaseThenReallocate(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D5 Release 后可重新 Allocate")

	node := h.registerGame("game-1", "session-a")
	grainID := h.grainID("d5")
	placement := h.allocate(grainID)

	h.must(h.dir.Release(h.ctx, sp.ReleaseCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
	}), "Release")

	_, err := h.dir.Lookup(h.ctx, placement.GrainKey)
	h.mustErrIs(err, sp.ErrPlacementNotFound, "Lookup after release")

	reallocated := h.allocate(grainID)
	if reallocated.Status != sp.PlacementStatusActive {
		t.Fatalf("status = %s", reallocated.Status)
	}
}

func TestPlacement_D6_RecoverRejectedAfterRelease(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D6 Release 后 Recover 返回 NotRecoverable")

	node1 := h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")
	placement := h.allocate(h.grainID("d6"))

	h.must(h.dir.Release(h.ctx, sp.ReleaseCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node1.NodeIdentity,
		NodeSessionID:    node1.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
	}), "Release")

	_, err := h.dir.Recover(h.ctx, sp.RecoverCommand{
		GrainKey:         placement.GrainKey,
		NewNodeIdentity:  node2.NodeIdentity,
		PlacementVersion: placement.Version,
		LeaseTTL:         time.Minute,
	})
	h.mustErrIs(err, sp.ErrPlacementNotRecoverable, "Recover after release")
}

func TestPlacement_D7_TransferChangesOwner(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D7 Transfer 显式更换 Owner")

	h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")
	placement := h.allocate(h.grainID("d7"))

	transferred, err := h.dir.Transfer(h.ctx, sp.TransferCommand{
		GrainKey:         placement.GrainKey,
		FromNodeIdentity: placement.NodeIdentity,
		ToNodeIdentity:   node2.NodeIdentity,
		PlacementVersion: placement.Version,
		LeaseTTL:         time.Minute,
	})
	h.must(err, "Transfer")
	if transferred.NodeIdentity != node2.NodeIdentity || transferred.Version != placement.Version+1 {
		t.Fatalf("transferred = %+v", transferred)
	}
	if len(h.placementsOn(node2)) != 1 {
		t.Fatalf("game-2 placements = %d", len(h.placementsOn(node2)))
	}
}

func TestPlacement_D8_ExpireThenRecover(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D8 Expire 后 Recover 恢复 Active")

	h.registerGame("game-1", "session-a")
	node2 := h.registerGame("game-2", "session-b")
	placement := h.allocateWithTTL(h.grainID("d8"), time.Second)

	h.must(h.dir.Expire(h.ctx, sp.ExpireCommand{
		GrainKey:     placement.GrainKey,
		LeaseVersion: placement.Lease.Version,
		Now:          placement.LeaseExpireAt.Add(time.Millisecond),
	}), "Expire")

	_, err := h.dir.Lookup(h.ctx, placement.GrainKey)
	h.mustErrIs(err, sp.ErrPlacementNotFound, "Lookup after expire")

	recovered, err := h.dir.Recover(h.ctx, sp.RecoverCommand{
		GrainKey:         placement.GrainKey,
		NewNodeIdentity:  node2.NodeIdentity,
		PlacementVersion: placement.Version,
		LeaseTTL:         time.Minute,
	})
	h.must(err, "Recover")
	if recovered.Status != sp.PlacementStatusActive {
		t.Fatalf("status = %s", recovered.Status)
	}
}

func TestPlacement_D9_ExpireBeforeLeaseEndFails(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D9 Expire 租约未到期失败")

	h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("d9"))

	err := h.dir.Expire(h.ctx, sp.ExpireCommand{
		GrainKey:     placement.GrainKey,
		LeaseVersion: placement.Lease.Version,
		Now:          placement.LeaseExpireAt.Add(-time.Second),
	})
	h.mustErrIs(err, sp.ErrLeaseNotExpired, "Expire early")
}

func TestPlacement_D10_ExistsOnlyForActive(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("D10 Exists 仅对 Active Placement 为 true")

	node := h.registerGame("game-1", "session-a")
	placement := h.allocate(h.grainID("d10"))

	ok, err := h.dir.Exists(h.ctx, placement.GrainKey)
	h.must(err, "Exists active")
	if !ok {
		t.Fatal("Exists = false for active placement")
	}

	h.must(h.dir.Release(h.ctx, sp.ReleaseCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
	}), "Release")

	ok, err = h.dir.Exists(h.ctx, placement.GrainKey)
	h.must(err, "Exists released")
	if ok {
		t.Fatal("Exists = true after release")
	}
}
