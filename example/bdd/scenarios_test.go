//go:build integration

package bdd_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

func TestStablePlacementRedisBDD(t *testing.T) {
	t.Run("注册节点后分配并 Lookup", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("注册节点后分配 Placement")
		node1 := s.registerNode("game-1", "session-a")
		s.registerNode("game-2", "session-b")

		grainID := s.grainID("allocate")
		s.step("When Allocate Player/%s", grainID)
		placement := s.allocate(grainID)

		s.step("Then Lookup 返回同一 Owner")
		found, err := s.dir.Lookup(s.ctx, placement.GrainKey)
		if err != nil {
			t.Fatalf("Lookup error: %v", err)
		}
		if found.NodeIdentity != placement.NodeIdentity {
			t.Fatalf("lookup node = %q, want %q", found.NodeIdentity, placement.NodeIdentity)
		}
		if placement.NodeIdentity != node1.NodeIdentity {
			t.Fatalf("allocated node = %q, want %q", placement.NodeIdentity, node1.NodeIdentity)
		}
	})

	t.Run("Owner Renew 只校验当前 Placement", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("Owner 续约")
		node := s.registerNode("game-1", "session-a")

		grainID := s.grainID("renew")
		placement := s.allocate(grainID)
		leaseBefore := s.dirNode(node.NodeIdentity).Lease

		s.step("When Renew")
		renewed, err := s.dir.Renew(s.ctx, sp.RenewCommand{
			GrainKey:         placement.GrainKey,
			NodeIdentity:     placement.NodeIdentity,
			NodeSessionID:    placement.OwnerNodeSessionID,
			PlacementVersion: placement.Version,
		})
		if err != nil {
			t.Fatalf("Renew error: %v", err)
		}
		s.step("Then Placement 与 Node Lease 均不变")
		if renewed.Version != placement.Version || renewed.OwnerNodeSessionID != placement.OwnerNodeSessionID {
			t.Fatalf("renewed = %+v, want unchanged %+v", renewed, placement)
		}
		if leaseAfter := s.dirNode(node.NodeIdentity).Lease; leaseAfter != leaseBefore {
			t.Fatalf("Renew changed Node Lease: before=%+v after=%+v", leaseBefore, leaseAfter)
		}
	})

	t.Run("释放后禁止 Recover", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("Release 后 Recover 应失败")
		s.registerNode("game-1", "session-a")
		node2 := s.registerNode("game-2", "session-b")

		grainID := s.grainID("release-recover")
		placement := s.allocate(grainID)

		s.step("When Release")
		if err := s.dir.Release(s.ctx, sp.ReleaseCommand{
			GrainKey:         placement.GrainKey,
			NodeIdentity:     placement.NodeIdentity,
			NodeSessionID:    placement.OwnerNodeSessionID,
			PlacementVersion: placement.Version,
		}); err != nil {
			t.Fatalf("Release error: %v", err)
		}

		s.step("Then Recover 返回 ErrPlacementNotRecoverable")
		_, err := s.dir.Recover(s.ctx, sp.RecoverCommand{
			GrainKey:         placement.GrainKey,
			NewNodeIdentity:  node2.NodeIdentity,
			PlacementVersion: placement.Version + 1,
		})
		if !errors.Is(err, sp.ErrPlacementNotRecoverable) {
			t.Fatalf("Recover err = %v, want ErrPlacementNotRecoverable", err)
		}
	})

	t.Run("Node Lease 到期使同节点所有 Grain 逻辑失效但保留 Placement", func(t *testing.T) {
		s := newSuiteWithNodeLeaseConfig(t, sp.NodeLeaseConfig{TTL: 100 * time.Millisecond})
		defer s.cleanup()
		s.scenario("一个 Node Lease 控制该 session 的全部 Grain 路由")
		node := s.registerNode("game-1", "session-a")
		placements := []*sp.Placement{
			s.allocate(s.grainID("node-lease-1")),
			s.allocate(s.grainID("node-lease-2")),
		}
		s.waitForLookupError(placements[0].GrainKey, sp.ErrPlacementNotFound)

		for _, placement := range placements {
			_, err := s.dir.Lookup(s.ctx, placement.GrainKey)
			if !errors.Is(err, sp.ErrPlacementNotFound) {
				t.Fatalf("Lookup %s err = %v, want ErrPlacementNotFound", placement.GrainKey, err)
			}
			exists, err := s.dir.Exists(s.ctx, placement.GrainKey)
			if err != nil || exists {
				t.Fatalf("Exists %s = %v, err=%v", placement.GrainKey, exists, err)
			}
		}
		page, err := s.dir.FindByNode(s.ctx, sp.FindByNodeQuery{NodeIdentity: node.NodeIdentity, Limit: 10})
		if err != nil || len(page.Placements) != len(placements) {
			t.Fatalf("FindByNode = %+v, err=%v", page, err)
		}
		for _, placement := range page.Placements {
			if placement.Status != sp.PlacementStatusActive || placement.OwnerNodeSessionID != node.NodeSessionID {
				t.Fatalf("retained placement = %+v", placement)
			}
		}
	})

	t.Run("健康 Owner 拒绝 Recover", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("健康 Owner 只能通过 Transfer 显式迁移")
		s.registerNode("game-1", "session-a")
		node2 := s.registerNode("game-2", "session-b")
		placement := s.allocate(s.grainID("healthy-recover"))

		_, err := s.dir.Recover(s.ctx, sp.RecoverCommand{
			GrainKey:         placement.GrainKey,
			NewNodeIdentity:  node2.NodeIdentity,
			PlacementVersion: placement.Version,
		})
		if !errors.Is(err, sp.ErrPlacementNotRecoverable) {
			t.Fatalf("Recover err = %v, want ErrPlacementNotRecoverable", err)
		}
	})

	t.Run("失效节点不参与新分配", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("MarkNodeInvalid 后 Allocate 跳过该 NodeName")
		node1 := s.registerNode("game-1", "session-a")
		node2 := s.registerNode("game-2", "session-b")

		s.step("Given MarkNodeInvalid game-1")
		if err := s.dir.MarkNodeInvalid(s.ctx, s.nodeType, s.nodeGroup, "game-1"); err != nil {
			t.Fatalf("MarkNodeInvalid error: %v", err)
		}

		grainID := s.grainID("invalid-node")
		placement := s.allocate(grainID)

		s.step("Then 分配到 game-2")
		if placement.NodeIdentity == node1.NodeIdentity || placement.NodeIdentity != node2.NodeIdentity {
			t.Fatalf("allocated node = %q, want %q", placement.NodeIdentity, node2.NodeIdentity)
		}
	})

	t.Run("显式 Transfer 迁移", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("Transfer 更换 Owner")
		s.registerNode("game-1", "session-a")
		node2 := s.registerNode("game-2", "session-b")

		grainID := s.grainID("transfer")
		placement := s.allocate(grainID)

		s.step("When Transfer 到 game-2")
		transferred, err := s.dir.Transfer(s.ctx, sp.TransferCommand{
			GrainKey:         placement.GrainKey,
			FromNodeIdentity: placement.NodeIdentity,
			ToNodeIdentity:   node2.NodeIdentity,
			PlacementVersion: placement.Version,
		})
		if err != nil {
			t.Fatalf("Transfer error: %v", err)
		}
		s.step("Then Owner 为 game-2")
		if transferred.NodeIdentity != node2.NodeIdentity {
			t.Fatalf("transferred node = %q", transferred.NodeIdentity)
		}
	})

	t.Run("幂等分配", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("同一 Grain 并发 Allocate 仅一个 Active Placement")
		s.registerNode("game-1", "session-a")

		grainID := s.grainID("idempotent")
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := s.dir.Allocate(s.ctx, sp.AllocateCommand{
					GrainID:         grainID,
					Kind:            "Player",
					TargetNodeType:  s.nodeType,
					TargetNodeGroup: s.nodeGroup,
				})
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("Allocate error: %v", err)
			}
		}

		key, _ := sp.NewGrainKey("Player", grainID)
		ok, err := s.dir.Exists(s.ctx, key)
		if err != nil || !ok {
			t.Fatalf("Exists = %v err = %v", ok, err)
		}
	})
}
