package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type evalHookClient struct {
	goredis.UniversalClient
	beforeEval func(script string)
}

func (c evalHookClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *goredis.Cmd {
	if c.beforeEval != nil {
		c.beforeEval(script)
	}
	return c.UniversalClient.Eval(ctx, script, keys, args...)
}

type failXAddClient struct {
	goredis.UniversalClient
	err error
}

func (c failXAddClient) XAdd(ctx context.Context, a *goredis.XAddArgs) *goredis.StringCmd {
	return goredis.NewStringResult("", c.err)
}

func newTestDirectory(t *testing.T) (*Directory, *goredis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	dir, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	return dir, client
}

func TestRedisDirectoryRejectsGoStrategyMode(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	_, err := NewDirectory(client, sp.StrategyModeGo)
	if !errors.Is(err, sp.ErrUnsupportedStrategyMode) {
		t.Fatalf("NewDirectory err = %v, want ErrUnsupportedStrategyMode", err)
	}
}

var (
	_ sp.Directory    = (*Directory)(nil)
	_ sp.NodeRegistry = (*Directory)(nil)
)

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

func TestRedisDirectoryTransferRecoverAndExpire(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	node1 := registerTestNode(t, dir, "game-1", "session-a")
	node2 := registerTestNode(t, dir, "game-2", "session-b")

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

	transferred, err := dir.Transfer(ctx, sp.TransferCommand{
		GrainKey:         placement.GrainKey,
		FromNodeIdentity: placement.NodeIdentity,
		ToNodeIdentity:   node2.NodeIdentity,
		PlacementVersion: placement.Version,
		LeaseTTL:         time.Minute,
	})
	if err != nil {
		t.Fatalf("Transfer error: %v", err)
	}
	if transferred.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("transferred node = %q", transferred.NodeIdentity)
	}
	if transferred.Version != placement.Version+1 {
		t.Fatalf("transferred version = %d", transferred.Version)
	}
	if transferred.Lease.OwnerNodeSessionID != node2.NodeSessionID || transferred.Lease.Version != 1 {
		t.Fatalf("transferred lease = %+v", transferred.Lease)
	}

	oldPage, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: placement.NodeIdentity, Limit: 10})
	if err != nil {
		t.Fatalf("FindByNode old error: %v", err)
	}
	if len(oldPage.Placements) != 0 {
		t.Fatalf("old node page = %+v", oldPage)
	}
	newPage, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: node2.NodeIdentity, Limit: 10})
	if err != nil {
		t.Fatalf("FindByNode new error: %v", err)
	}
	if len(newPage.Placements) != 1 || newPage.Placements[0].GrainKey != placement.GrainKey {
		t.Fatalf("new node page = %+v", newPage)
	}

	if err := dir.Release(ctx, sp.ReleaseCommand{
		GrainKey:         transferred.GrainKey,
		NodeIdentity:     transferred.NodeIdentity,
		NodeSessionID:    transferred.Lease.OwnerNodeSessionID,
		PlacementVersion: transferred.Version,
		LeaseVersion:     transferred.Lease.Version,
	}); err != nil {
		t.Fatalf("Release error: %v", err)
	}
	_, err = dir.Recover(ctx, sp.RecoverCommand{
		GrainKey:         transferred.GrainKey,
		NewNodeIdentity:  node1.NodeIdentity,
		PlacementVersion: transferred.Version,
		LeaseTTL:         time.Minute,
	})
	if !errors.Is(err, sp.ErrPlacementNotRecoverable) {
		t.Fatalf("Recover after release err = %v, want ErrPlacementNotRecoverable", err)
	}

	expiring, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10002",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Second,
	})
	if err != nil {
		t.Fatalf("Allocate expiring error: %v", err)
	}
	if err := dir.Expire(ctx, sp.ExpireCommand{
		GrainKey:     expiring.GrainKey,
		LeaseVersion: expiring.Lease.Version,
		Now:          expiring.LeaseExpireAt.Add(-time.Millisecond),
	}); !errors.Is(err, sp.ErrLeaseNotExpired) {
		t.Fatalf("Expire before lease end err = %v", err)
	}
	if err := dir.Expire(ctx, sp.ExpireCommand{
		GrainKey:     expiring.GrainKey,
		LeaseVersion: expiring.Lease.Version,
		Now:          expiring.LeaseExpireAt.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("Expire error: %v", err)
	}
	if _, err := dir.Lookup(ctx, expiring.GrainKey); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Lookup after expire err = %v", err)
	}

	faulty, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10003",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Second,
	})
	if err != nil {
		t.Fatalf("Allocate faulty error: %v", err)
	}
	if err := dir.Expire(ctx, sp.ExpireCommand{
		GrainKey:     faulty.GrainKey,
		LeaseVersion: faulty.Lease.Version,
		Now:          faulty.LeaseExpireAt.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("Expire faulty error: %v", err)
	}
	recovered, err := dir.Recover(ctx, sp.RecoverCommand{
		GrainKey:         faulty.GrainKey,
		NewNodeIdentity:  node1.NodeIdentity,
		PlacementVersion: faulty.Version,
		LeaseTTL:         time.Minute,
	})
	if err != nil {
		t.Fatalf("Recover after expire error: %v", err)
	}
	if recovered.Status != sp.PlacementStatusActive || recovered.NodeIdentity != node1.NodeIdentity {
		t.Fatalf("recovered placement = %+v", recovered)
	}

	events := client.XRange(ctx, EventsStreamKey(), "-", "+").Val()
	var sawTransfer, sawRecover, sawExpire bool
	for _, event := range events {
		switch event.Values["type"] {
		case string(sp.EventPlacementTransferred):
			sawTransfer = true
		case string(sp.EventPlacementRecovered):
			sawRecover = true
		case string(sp.EventLeaseExpired):
			sawExpire = true
		}
	}
	if !sawTransfer || !sawRecover || !sawExpire {
		t.Fatalf("stream missing events: transfer=%v recover=%v expire=%v", sawTransfer, sawRecover, sawExpire)
	}
}

func TestRedisDirectoryFindByNodeCursorUsesStableScore(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	node := registerTestNode(t, dir, "game-1", "session-a")
	var placements []*sp.Placement
	for _, grainID := range []string{"10001", "10002", "10003"} {
		placement, err := dir.Allocate(ctx, sp.AllocateCommand{
			GrainID:         grainID,
			Kind:            "Player",
			TargetNodeType:  "game",
			TargetNodeGroup: "default",
			LeaseTTL:        time.Minute,
		})
		if err != nil {
			t.Fatalf("Allocate %s error: %v", grainID, err)
		}
		placements = append(placements, placement)
	}

	first, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: node.NodeIdentity, Limit: 1})
	if err != nil {
		t.Fatalf("FindByNode first error: %v", err)
	}
	if len(first.Placements) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %+v", first)
	}
	if err := dir.Release(ctx, sp.ReleaseCommand{
		GrainKey:         placements[0].GrainKey,
		NodeIdentity:     placements[0].NodeIdentity,
		NodeSessionID:    placements[0].Lease.OwnerNodeSessionID,
		PlacementVersion: placements[0].Version,
		LeaseVersion:     placements[0].Lease.Version,
	}); err != nil {
		t.Fatalf("Release first page placement error: %v", err)
	}

	second, err := dir.FindByNode(ctx, sp.FindByNodeQuery{NodeIdentity: node.NodeIdentity, Cursor: first.NextCursor, Limit: 10})
	if err != nil {
		t.Fatalf("FindByNode second error: %v", err)
	}
	if len(second.Placements) != 2 {
		t.Fatalf("second page after index change = %+v", second)
	}
}

func TestRedisDirectoryNodeRegistryLifecycle(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	node := registerTestNode(t, dir, "game-1", "session-a")

	if err := dir.RenewNode(ctx, node.NodeIdentity, "wrong-session"); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("RenewNode wrong session err = %v", err)
	}
	if err := dir.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatalf("RenewNode error: %v", err)
	}

	replacement := node
	replacement.NodeSessionID = "session-b"
	old, err := dir.ReplaceNodeSession(ctx, replacement)
	if err != nil {
		t.Fatalf("ReplaceNodeSession error: %v", err)
	}
	if old.NodeSessionID != "session-a" {
		t.Fatalf("old session = %+v", old)
	}
	if _, err := dir.Renew(ctx, sp.RenewCommand{
		GrainKey:      sp.GrainKey("missing"),
		NodeIdentity:  node.NodeIdentity,
		NodeSessionID: "session-a",
	}); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Renew missing placement err = %v", err)
	}

	if err := dir.DrainNode(ctx, node.NodeIdentity); !errors.Is(err, sp.ErrNodeNotInvalid) {
		t.Fatalf("DrainNode before invalid err = %v", err)
	}
	if err := dir.MarkNodeInvalid(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}
	invalid, err := dir.ListInvalidNodes(ctx, node.NodeType, node.NodeGroup)
	if err != nil {
		t.Fatalf("ListInvalidNodes error: %v", err)
	}
	if len(invalid) != 1 || invalid[0] != node.NodeName {
		t.Fatalf("invalid nodes = %+v", invalid)
	}
	if err := dir.DrainNode(ctx, node.NodeIdentity); err != nil {
		t.Fatalf("DrainNode error: %v", err)
	}
	nodes, err := dir.FindNodes(ctx, node.NodeType, node.NodeGroup)
	if err != nil {
		t.Fatalf("FindNodes error: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Status != sp.NodeStatusDraining {
		t.Fatalf("nodes after drain = %+v", nodes)
	}
	if err := dir.RestoreNode(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatalf("RestoreNode error: %v", err)
	}
	if err := dir.CompleteDrain(ctx, node.NodeIdentity, replacement.NodeSessionID); err != nil {
		t.Fatalf("CompleteDrain error: %v", err)
	}
	nodes, err = dir.FindNodes(ctx, node.NodeType, node.NodeGroup)
	if err != nil {
		t.Fatalf("FindNodes after complete drain error: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("nodes after complete drain = %+v", nodes)
	}
}

func TestRedisDirectoryAllocateWritesOneOutboxEventUnderRace(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	dir, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	registerTestNode(t, dir, "game-1", "session-a")

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := dir.Allocate(ctx, sp.AllocateCommand{
				GrainID:         "10001",
				Kind:            "Player",
				TargetNodeType:  "game",
				TargetNodeGroup: "default",
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

	events := client.XRange(ctx, EventsStreamKey(), "-", "+").Val()
	created := 0
	for _, event := range events {
		if event.Values["type"] == string(sp.EventPlacementCreated) {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("PlacementCreated events = %d, want 1", created)
	}
}

func TestRedisDirectoryRenewIsVersionedAndWritesAuditOnly(t *testing.T) {
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
	eventsBefore := client.XLen(ctx, EventsStreamKey()).Val()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := dir.Renew(ctx, sp.RenewCommand{
				GrainKey:         placement.GrainKey,
				NodeIdentity:     node.NodeIdentity,
				NodeSessionID:    node.NodeSessionID,
				PlacementVersion: placement.Version,
				LeaseVersion:     placement.Lease.Version,
				ExtendTTL:        time.Minute,
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	successes := 0
	conflicts := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, sp.ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("Renew err = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("renew successes=%d conflicts=%d", successes, conflicts)
	}
	if eventsAfter := client.XLen(ctx, EventsStreamKey()).Val(); eventsAfter != eventsBefore {
		t.Fatalf("renew wrote cache invalidation events: before=%d after=%d", eventsBefore, eventsAfter)
	}
	audit := client.XRange(ctx, AuditStreamKey(), "-", "+").Val()
	if len(audit) != 1 || audit[0].Values["type"] != string(sp.EventPlacementRenewed) {
		t.Fatalf("audit stream = %+v", audit)
	}
}

func TestRedisDirectoryAllocateLuaRejectsAllInvalidCandidates(t *testing.T) {
	ctx := context.Background()
	dir, _ := newTestDirectory(t)
	registerTestNode(t, dir, "game-1", "session-a")
	if err := dir.MarkNodeInvalid(ctx, "game", "default", "game-1"); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}

	_, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if !errors.Is(err, sp.ErrNoAvailableNode) {
		t.Fatalf("Allocate err = %v, want ErrNoAvailableNode", err)
	}
	key, _ := sp.NewGrainKey("Player", "10001")
	if _, err := dir.Lookup(ctx, key); !errors.Is(err, sp.ErrPlacementNotFound) {
		t.Fatalf("Lookup after rejected allocate err = %v", err)
	}
}

func TestRedisDirectoryAllocateUsesLuaRoundRobinInsteadOfGoStrategy(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	dir, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	node1 := registerTestNode(t, dir, "game-1", "session-a")
	node2 := registerTestNode(t, dir, "game-2", "session-b")

	first, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("first Allocate error: %v", err)
	}
	second, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10002",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("second Allocate error: %v", err)
	}
	if first.NodeIdentity != node1.NodeIdentity {
		t.Fatalf("first node = %q, want %q", first.NodeIdentity, node1.NodeIdentity)
	}
	if second.NodeIdentity != node2.NodeIdentity {
		t.Fatalf("second node = %q, want %q", second.NodeIdentity, node2.NodeIdentity)
	}
}

func TestRedisDirectoryRenewLuaRejectsSessionReplacedAfterGoValidation(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	var dir *Directory
	var err error
	var once sync.Once
	client := evalHookClient{
		UniversalClient: base,
		beforeEval: func(script string) {
			if script != renewLua {
				return
			}
			once.Do(func() {
				replacement := sp.Node{
					NodeType:      "game",
					NodeGroup:     "default",
					NodeName:      "game-1",
					NodeIdentity:  "game/default/game-1",
					NodeSessionID: "session-b",
					Status:        sp.NodeStatusActive,
				}
				if _, err := dir.ReplaceNodeSession(ctx, replacement); err != nil {
					t.Fatalf("ReplaceNodeSession in hook error: %v", err)
				}
			})
		},
	}
	dir, err = NewDirectory(client, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
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

	_, err = dir.Renew(ctx, sp.RenewCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
		ExtendTTL:        time.Minute,
	})
	if !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("Renew err = %v, want ErrInvalidNodeSession", err)
	}
}

func TestRedisDirectoryReleaseLuaRejectsSessionReplacedAfterGoValidation(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	var dir *Directory
	var err error
	var once sync.Once
	client := evalHookClient{
		UniversalClient: base,
		beforeEval: func(script string) {
			if script != mutationLua {
				return
			}
			once.Do(func() {
				replacement := sp.Node{
					NodeType:      "game",
					NodeGroup:     "default",
					NodeName:      "game-1",
					NodeIdentity:  "game/default/game-1",
					NodeSessionID: "session-b",
					Status:        sp.NodeStatusActive,
				}
				if _, err := dir.ReplaceNodeSession(ctx, replacement); err != nil {
					t.Fatalf("ReplaceNodeSession in hook error: %v", err)
				}
			})
		},
	}
	dir, err = NewDirectory(client, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
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

	err = dir.Release(ctx, sp.ReleaseCommand{
		GrainKey:         placement.GrainKey,
		NodeIdentity:     node.NodeIdentity,
		NodeSessionID:    node.NodeSessionID,
		PlacementVersion: placement.Version,
		LeaseVersion:     placement.Lease.Version,
	})
	if !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("Release err = %v, want ErrInvalidNodeSession", err)
	}
	found, err := dir.Lookup(ctx, placement.GrainKey)
	if err != nil {
		t.Fatalf("Lookup after rejected release error: %v", err)
	}
	if found.Status != sp.PlacementStatusActive {
		t.Fatalf("placement status = %s", found.Status)
	}
}

func TestRedisDirectoryNodeRegistryWritesEventsThroughLua(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	client := failXAddClient{UniversalClient: base, err: errors.New("direct xadd disabled")}
	dir, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	node := sp.Node{
		NodeType:      "game",
		NodeGroup:     "default",
		NodeName:      "game-1",
		NodeIdentity:  "game/default/game-1",
		NodeSessionID: "session-a",
		Status:        sp.NodeStatusActive,
	}

	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatalf("RegisterNode error: %v", err)
	}
	replacement := node
	replacement.NodeSessionID = "session-b"
	old, err := dir.ReplaceNodeSession(ctx, replacement)
	if err != nil {
		t.Fatalf("ReplaceNodeSession error: %v", err)
	}
	if old.NodeSessionID != "session-a" {
		t.Fatalf("old node = %+v", old)
	}
	if err := dir.MarkNodeInvalid(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatalf("MarkNodeInvalid error: %v", err)
	}
	if err := dir.RestoreNode(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatalf("RestoreNode error: %v", err)
	}
	if err := dir.MarkNodeInvalid(ctx, node.NodeType, node.NodeGroup, node.NodeName); err != nil {
		t.Fatalf("MarkNodeInvalid before drain error: %v", err)
	}
	if err := dir.DrainNode(ctx, node.NodeIdentity); err != nil {
		t.Fatalf("DrainNode error: %v", err)
	}
	if err := dir.CompleteDrain(ctx, node.NodeIdentity, replacement.NodeSessionID); err != nil {
		t.Fatalf("CompleteDrain error: %v", err)
	}

	events := base.XRange(ctx, EventsStreamKey(), "-", "+").Val()
	want := []sp.EventType{
		sp.EventNodeRegistered,
		sp.EventNodeReplaced,
		sp.EventNodeMarkedInvalid,
		sp.EventNodeRestored,
		sp.EventNodeMarkedInvalid,
		sp.EventNodeDraining,
		sp.EventNodeUnregistered,
	}
	if len(events) != len(want) {
		t.Fatalf("events len = %d, want %d: %+v", len(events), len(want), events)
	}
	for i, eventType := range want {
		if events[i].Values["type"] != string(eventType) {
			t.Fatalf("event[%d] type = %v, want %s", i, events[i].Values["type"], eventType)
		}
	}
}

func TestRedisDirectoryAllocateAdvancesRoundRobinCursorOnlyWhenCreated(t *testing.T) {
	ctx := context.Background()
	dir, client := newTestDirectory(t)
	registerTestNode(t, dir, "game-1", "session-a")
	registerTestNode(t, dir, "game-2", "session-b")
	rrKey := StrategyRoundRobinKey("game", "default")

	if _, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	}); err != nil {
		t.Fatalf("first Allocate error: %v", err)
	}
	if got := client.Get(ctx, rrKey).Val(); got != "1" {
		t.Fatalf("rr cursor after first allocate = %q, want 1", got)
	}

	if _, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10001",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	}); err != nil {
		t.Fatalf("existing Allocate error: %v", err)
	}
	if got := client.Get(ctx, rrKey).Val(); got != "1" {
		t.Fatalf("rr cursor after existing allocate = %q, want 1", got)
	}

	if _, err := dir.Allocate(ctx, sp.AllocateCommand{
		GrainID:         "10002",
		Kind:            "Player",
		TargetNodeType:  "game",
		TargetNodeGroup: "default",
		LeaseTTL:        time.Minute,
	}); err != nil {
		t.Fatalf("second new Allocate error: %v", err)
	}
	if got := client.Get(ctx, rrKey).Val(); got != "2" {
		t.Fatalf("rr cursor after second new allocate = %q, want 2", got)
	}
}
