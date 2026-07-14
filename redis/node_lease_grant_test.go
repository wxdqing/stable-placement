package redis

import (
	"context"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

func TestRedisDirectoryReturnsNodeLeaseGrants(t *testing.T) {
	ctx := context.Background()
	dir, _, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: 10 * time.Second})
	server.SetTime(time.Unix(1_000, 0))
	node := testNode("game-1", "session-a")

	registered, err := dir.RegisterNode(ctx, node)
	if err != nil || registered.Version != 1 || registered.ValidUntil.IsZero() {
		t.Fatalf("RegisterNode grant=%+v err=%v", registered, err)
	}
	renewed, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID})
	if err != nil || renewed.Version != 2 || renewed.ValidUntil.IsZero() {
		t.Fatalf("RenewNode grant=%+v err=%v", renewed, err)
	}
	bad, err := dir.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: "old-session"})
	if err == nil || bad != (sp.NodeLeaseGrant{}) {
		t.Fatalf("failed RenewNode grant=%+v err=%v", bad, err)
	}
	replacement := node
	replacement.NodeSessionID = "session-b"
	old, replaced, err := dir.ReplaceNodeSession(ctx, replacement)
	if err != nil || old == nil || old.NodeSessionID != "session-a" || replaced.Version != 1 || replaced.ValidUntil.IsZero() {
		t.Fatalf("ReplaceNodeSession old=%+v grant=%+v err=%v", old, replaced, err)
	}
}
