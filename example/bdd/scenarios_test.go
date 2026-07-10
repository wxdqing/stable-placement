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

	t.Run("Owner 续约推进 LeaseVersion", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("Owner 续约")
		node := s.registerNode("game-1", "session-a")

		grainID := s.grainID("renew")
		placement := s.allocate(grainID)

		s.step("When Renew")
		renewed, err := s.dir.Renew(s.ctx, sp.RenewCommand{
			GrainKey:         placement.GrainKey,
			NodeIdentity:     node.NodeIdentity,
			NodeSessionID:    node.NodeSessionID,
			PlacementVersion: placement.Version,
			LeaseVersion:     placement.Lease.Version,
			ExtendTTL:        time.Minute,
		})
		if err != nil {
			t.Fatalf("Renew error: %v", err)
		}
		s.step("Then LeaseVersion +1")
		if renewed.Lease.Version != placement.Lease.Version+1 {
			t.Fatalf("lease version = %d, want %d", renewed.Lease.Version, placement.Lease.Version+1)
		}
	})

	t.Run("释放后禁止 Recover", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("Release 后 Recover 应失败")
		node := s.registerNode("game-1", "session-a")
		node2 := s.registerNode("game-2", "session-b")

		grainID := s.grainID("release-recover")
		placement := s.allocate(grainID)

		s.step("When Release")
		if err := s.dir.Release(s.ctx, sp.ReleaseCommand{
			GrainKey:         placement.GrainKey,
			NodeIdentity:     node.NodeIdentity,
			NodeSessionID:    node.NodeSessionID,
			PlacementVersion: placement.Version,
			LeaseVersion:     placement.Lease.Version,
		}); err != nil {
			t.Fatalf("Release error: %v", err)
		}

		s.step("Then Recover 返回 ErrPlacementNotRecoverable")
		_, err := s.dir.Recover(s.ctx, sp.RecoverCommand{
			GrainKey:         placement.GrainKey,
			NewNodeIdentity:  node2.NodeIdentity,
			PlacementVersion: placement.Version,
			LeaseTTL:         time.Minute,
		})
		if !errors.Is(err, sp.ErrPlacementNotRecoverable) {
			t.Fatalf("Recover err = %v, want ErrPlacementNotRecoverable", err)
		}
	})

	t.Run("过期后可 Recover", func(t *testing.T) {
		s := newSuite(t)
		defer s.cleanup()
		s.scenario("Expire 后 Recover 恢复 Active")
		s.registerNode("game-1", "session-a")
		node2 := s.registerNode("game-2", "session-b")

		grainID := s.grainID("expire-recover")
		placement := s.allocate(grainID)

		s.step("When Expire")
		if err := s.dir.Expire(s.ctx, sp.ExpireCommand{
			GrainKey:     placement.GrainKey,
			LeaseVersion: placement.Lease.Version,
			Now:          placement.LeaseExpireAt.Add(time.Millisecond),
		}); err != nil {
			t.Fatalf("Expire error: %v", err)
		}

		s.step("Then Recover 到新节点")
		recovered, err := s.dir.Recover(s.ctx, sp.RecoverCommand{
			GrainKey:         placement.GrainKey,
			NewNodeIdentity:  node2.NodeIdentity,
			PlacementVersion: placement.Version,
			LeaseTTL:         time.Minute,
		})
		if err != nil {
			t.Fatalf("Recover error: %v", err)
		}
		if recovered.Status != sp.PlacementStatusActive || recovered.NodeIdentity != node2.NodeIdentity {
			t.Fatalf("recovered = %+v", recovered)
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
			LeaseTTL:         time.Minute,
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
					LeaseTTL:        time.Minute,
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
