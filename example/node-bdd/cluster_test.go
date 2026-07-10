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
	h.scenario("A3 RenewNode 刷新心跳，错误 session 被拒绝")

	node := h.registerGame("game-1", "session-a")
	before := h.listGameNodes()[0].LastHeartbeatAt

	h.step("When RenewNode 正确 session")
	time.Sleep(time.Millisecond)
	h.must(h.dir.RenewNode(h.ctx, node.NodeIdentity, node.NodeSessionID), "RenewNode")
	after := h.listGameNodes()[0].LastHeartbeatAt
	if !after.After(before) {
		t.Fatalf("LastHeartbeatAt = %v, want after %v", after, before)
	}

	h.step("Then 错误 session 返回 ErrInvalidNodeSession")
	err := h.dir.RenewNode(h.ctx, node.NodeIdentity, "wrong-session")
	h.mustErrIs(err, sp.ErrInvalidNodeSession, "RenewNode wrong session")
}
