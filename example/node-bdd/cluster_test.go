//go:build integration

package nodebdd_test

import (
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

// A. 节点集群基础

func TestCluster_A1_RegisterAndListGameNodes(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("A1 RegisterNode 后 FindNodes 可列出 game 节点")

	h.step("Given 注册 game-1, game-2")
	h.registerGame("game-1", "session-a")
	h.registerGame("game-2", "session-b")

	h.step("Then FindNodes 返回 2 个节点")
	h.assertSortedNodeList([]string{"game-1", "game-2"})
}

func TestCluster_A2_NodeListSortedByIdentity(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("A2 多节点列表按 NodeIdentity 稳定排序")

	h.registerGame("game-3", "s3")
	h.registerGame("game-1", "s1")
	h.registerGame("game-2", "s2")

	h.step("Then 按 NodeName 字典序排列")
	h.assertSortedNodeList([]string{"game-1", "game-2", "game-3"})
}

func TestCluster_A3_RenewNodeSessionValidation(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	h.scenario("A3 RenewNode 推进 Node Lease，错误 session 被拒绝")

	node := h.registerGame("game-1", "session-a")
	before := h.listGameNodes()[0].Lease

	h.step("When RenewNode 正确 session")
	deadline := time.Now().Add(3 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := h.dir.RenewNode(h.ctx, node.NodeIdentity, node.NodeSessionID)
		h.must(err, "RenewNode")
		after := h.listGameNodes()[0].Lease
		if after.Version > before.Version && after.ExpireAtUnixMilli > before.ExpireAtUnixMilli {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Node Lease did not advance beyond %+v", before)
		}
		<-ticker.C
	}

	h.step("Then 错误 session 返回 ErrInvalidNodeSession")
	_, err := h.dir.RenewNode(h.ctx, node.NodeIdentity, "wrong-session")
	h.mustErrIs(err, sp.ErrInvalidNodeSession, "RenewNode wrong session")
}
