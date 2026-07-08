package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/strategies"
)

func newTestDirectory(t *testing.T) (*Directory, *goredis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	dir := NewDirectory(client, strategies.NewRoundRobin())
	return dir, client
}

func registerTestNode(t *testing.T, dir *Directory, name string, session string) sp.Node {
	t.Helper()
	id, err := sp.NewNodeIdentity("game", "default", name)
	if err != nil {
		t.Fatal(err)
	}
	node := sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      name,
		NodeIdentity:  id.String(),
		NodeSessionID: session,
		Status:        sp.NodeStatusActive,
	}
	if err := dir.RegisterNode(context.Background(), node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	return node
}

func TestRedisDirectoryAllocateLookupRenewReleaseAndOutbox(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	node := registerTestNode(t, dir, "game-1", "session-a")

	placement, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	if placement.NodeIdentity != node.NodeIdentity {
		t.Fatalf("allocated node = %q", placement.NodeIdentity)
	}
	if streamLen := client.XLen(ctx, EventsStreamKey()).Val(); streamLen != 2 {
		t.Fatalf("stream len after allocate = %d", streamLen)
	}

	found, err := dir.Lookup(ctx, placement.GrainKey)
	if err != nil {
		t.Fatalf("Lookup error: %v", err)
	}
	if found.NodeIdentity != placement.NodeIdentity {
		t.Fatalf("lookup node = %q", found.NodeIdentity)
	}

	renewed, err := dir.Renew(ctx, sp.RenewCommand{
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
	if renewed.Lease.Version != placement.Lease.Version+1 {
		t.Fatalf("lease version = %d", renewed.Lease.Version)
	}
	if streamLen := client.XLen(ctx, EventsStreamKey()).Val(); streamLen != 2 {
		t.Fatalf("renew wrote cache invalidation stream, len = %d", streamLen)
	}

	if err := dir.Release(ctx, sp.ReleaseCommand{
		GrainKey:         renewed.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: renewed.Version,
		LeaseVersion:     renewed.Lease.Version,
	}); err != nil {
		t.Fatalf("Release error: %v", err)
	}
	if _, err := dir.Lookup(ctx, placement.GrainKey); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Lookup after release err = %v", err)
	}
	if streamLen := client.XLen(ctx, EventsStreamKey()).Val(); streamLen != 3 {
		t.Fatalf("stream len after release = %d", streamLen)
	}
}

func TestRedisDirectoryInvalidNodeAndFindByNode(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	node1 := registerTestNode(t, dir, "game-1", "session-a")
	node2 := registerTestNode(t, dir, "game-2", "session-b")
	if err := dir.MarkNodeInvalid(ctx, "game", "default", "game-1"); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}

	placement, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("Allocate error: %v", err)
	}
	if placement.NodeIdentity == node1.NodeIdentity || placement.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("allocated node = %q", placement.NodeIdentity)
	}

	page, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: node2.NodeIdentity, Limit: 10})
	if err != nil {
		t.Fatalf("FindByNode error: %v", err)
	}
	if len(page.Placements) != 1 || page.Placements[0].GrainKey != placement.GrainKey {
		t.Fatalf("page = %+v", page)
	}
}
