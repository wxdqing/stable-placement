package memory

import (
	"context"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

func TestNodeRegistryReturnsConservativeLeaseGrants(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(1_000, 0)
	clock := newFakeClock(start)
	registry := newTestRegistry(t, clock, nil, 10*time.Second)
	node := testNode("game-1", "session-a")

	registered, err := registry.RegisterNode(ctx, node)
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if registered.Version != 1 || !registered.ValidUntil.Equal(start.Add(10*time.Second)) {
		t.Fatalf("registered grant = %+v", registered)
	}

	clock.Advance(time.Second)
	renewed, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: node.NodeSessionID})
	if err != nil {
		t.Fatalf("RenewNode: %v", err)
	}
	if renewed.Version != 2 || !renewed.ValidUntil.Equal(clock.Now().Add(10*time.Second)) {
		t.Fatalf("renewed grant = %+v", renewed)
	}

	replacement := node
	replacement.NodeSessionID = "session-b"
	old, replaced, err := registry.ReplaceNodeSession(ctx, replacement)
	if err != nil {
		t.Fatalf("ReplaceNodeSession: %v", err)
	}
	if old == nil || old.NodeSessionID != node.NodeSessionID || replaced.Version != 1 || !replaced.ValidUntil.Equal(clock.Now().Add(10*time.Second)) {
		t.Fatalf("replace old=%+v grant=%+v", old, replaced)
	}

	bad, err := registry.RenewNode(ctx, sp.RenewNodeCommand{NodeIdentity: node.NodeIdentity, NodeSessionID: "old-session"})
	if err == nil || bad != (sp.NodeLeaseGrant{}) {
		t.Fatalf("failed renew grant=%+v err=%v", bad, err)
	}
}
