package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
)

type outsideCandidateStrategy struct{ node sp.Node }

func (s outsideCandidateStrategy) Choose(context.Context, sp.StrategyInput) (sp.Node, error) {
	return s.node, nil
}

func resolvePlayer(t *testing.T, directory *Directory, grainID, group string) (*sp.PlacementRoute, error) {
	t.Helper()
	return directory.ResolveRoute(context.Background(), sp.ResolveRouteCommand{
		GrainID: grainID, Kind: "player", TargetNodeType: "game", TargetNodeGroup: group,
	})
}

func TestResolveRouteAllocatesAndReusesHealthyOwner(t *testing.T) {
	directory, _, publisher := newTestDirectory(t)
	node := registerTestNode(t, directory, "game-1", "session-a")
	first, err := resolvePlayer(t, directory, "acct-1", "default")
	if err != nil {
		t.Fatalf("first ResolveRoute: %v", err)
	}
	second, err := resolvePlayer(t, directory, "acct-1", "default")
	if err != nil {
		t.Fatalf("second ResolveRoute: %v", err)
	}
	if first.NodeIdentity != node.NodeIdentity || first.OwnerNodeSessionID != node.NodeSessionID || *first != *second {
		t.Fatalf("routes first=%+v second=%+v", first, second)
	}
	if countEvents(publisher.Events(), sp.EventPlacementCreated) != 1 {
		t.Fatalf("events = %+v", publisher.Events())
	}
}

func TestResolveRouteRecoversSameNodeIdentityNewSession(t *testing.T) {
	directory, _, publisher := newTestDirectory(t)
	node := registerTestNode(t, directory, "game-1", "session-a")
	first, err := resolvePlayer(t, directory, "acct-1", "default")
	if err != nil {
		t.Fatal(err)
	}
	replacement := node
	replacement.NodeSessionID = "session-b"
	if _, _, err := directory.registry.ReplaceNodeSession(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	route, err := resolvePlayer(t, directory, "acct-1", "default")
	if err != nil {
		t.Fatalf("ResolveRoute after replacement: %v", err)
	}
	if route.NodeIdentity != node.NodeIdentity || route.OwnerNodeSessionID != "session-b" || route.Version != first.Version+1 {
		t.Fatalf("route = %+v, first = %+v", route, first)
	}
	if countEvents(publisher.Events(), sp.EventPlacementRecovered) != 1 {
		t.Fatalf("events = %+v", publisher.Events())
	}
}

func TestResolveRouteDoesNotCrossNodeBeforeManualInvalid(t *testing.T) {
	directory, _, _ := newTestDirectory(t)
	owner := registerTestNode(t, directory, "game-1", "session-a")
	registerTestNode(t, directory, "game-2", "session-b")
	first, err := resolvePlayer(t, directory, "acct-1", "default")
	if err != nil {
		t.Fatal(err)
	}
	setNodeStatus(directory.registry, owner.NodeIdentity, sp.NodeStatusOffline)
	route, err := resolvePlayer(t, directory, "acct-1", "default")
	if !errors.Is(err, sp.ErrPlacementOwnerUnavailable) || route != nil {
		t.Fatalf("ResolveRoute route=%+v err=%v", route, err)
	}
	stored := directory.placements[first.GrainKey]
	if stored.NodeIdentity != first.NodeIdentity || stored.Version != first.Version {
		t.Fatalf("placement changed = %+v", stored)
	}
}

func TestResolveRouteCrossesNodeOnlyAfterManualInvalid(t *testing.T) {
	directory, _, _ := newTestDirectory(t)
	owner := registerTestNode(t, directory, "game-1", "session-a")
	target := registerTestNode(t, directory, "game-2", "session-b")
	first, err := resolvePlayer(t, directory, "acct-1", "default")
	if err != nil {
		t.Fatal(err)
	}
	setNodeStatus(directory.registry, owner.NodeIdentity, sp.NodeStatusOffline)
	if err := directory.registry.MarkNodeInvalid(context.Background(), owner.NodeType, owner.NodeGroup, owner.NodeName); err != nil {
		t.Fatal(err)
	}
	route, err := resolvePlayer(t, directory, "acct-1", "default")
	if err != nil {
		t.Fatalf("ResolveRoute: %v", err)
	}
	if route.NodeIdentity != target.NodeIdentity || route.OwnerNodeSessionID != target.NodeSessionID || route.Version != first.Version+1 {
		t.Fatalf("route = %+v", route)
	}
}

func TestResolveRouteKeepsHealthyInvalidOwner(t *testing.T) {
	directory, _, _ := newTestDirectory(t)
	owner := registerTestNode(t, directory, "game-1", "session-a")
	registerTestNode(t, directory, "game-2", "session-b")
	first, err := resolvePlayer(t, directory, "acct-1", "default")
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.registry.MarkNodeInvalid(context.Background(), owner.NodeType, owner.NodeGroup, owner.NodeName); err != nil {
		t.Fatal(err)
	}
	route, err := resolvePlayer(t, directory, "acct-1", "default")
	if err != nil || *route != *first {
		t.Fatalf("route=%+v first=%+v err=%v", route, first, err)
	}
}

func TestResolveRouteRejectsTargetMismatch(t *testing.T) {
	directory, _, _ := newTestDirectory(t)
	registerTestNode(t, directory, "game-1", "session-a")
	if _, err := resolvePlayer(t, directory, "acct-1", "default"); err != nil {
		t.Fatal(err)
	}
	if route, err := resolvePlayer(t, directory, "acct-1", "other"); !errors.Is(err, sp.ErrPlacementTargetMismatch) || route != nil {
		t.Fatalf("route=%+v err=%v", route, err)
	}
}

func TestResolveRouteConcurrentCallsCreateOneOwner(t *testing.T) {
	directory, _, publisher := newTestDirectory(t)
	registerTestNode(t, directory, "game-1", "session-a")
	registerTestNode(t, directory, "game-2", "session-b")
	const callers = 100
	routes := make(chan *sp.PlacementRoute, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			route, err := resolvePlayer(t, directory, "acct-concurrent", "default")
			routes <- route
			errs <- err
		}()
	}
	wg.Wait()
	close(routes)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("ResolveRoute: %v", err)
		}
	}
	var first *sp.PlacementRoute
	for route := range routes {
		if first == nil {
			first = route
			continue
		}
		if *route != *first {
			t.Fatalf("route=%+v first=%+v", route, first)
		}
	}
	if countEvents(publisher.Events(), sp.EventPlacementCreated) != 1 {
		t.Fatalf("events = %+v", publisher.Events())
	}
}

func TestResolveRouteRejectsStrategyResultOutsideCandidates(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	registry := newTestRegistry(t, clock, nil, time.Minute)
	directory, err := NewDirectory(registry, sp.StrategyModeGo, outsideCandidateStrategy{node: testNode("outside", "session-x")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	registerTestNode(t, directory, "game-1", "session-a")
	route, err := resolvePlayer(t, directory, "acct-1", "default")
	if !errors.Is(err, sp.ErrNoAvailableNode) || route != nil {
		t.Fatalf("route=%+v err=%v", route, err)
	}
}

func countEvents(events []sp.PlacementEvent, eventType sp.EventType) int {
	count := 0
	for _, event := range events {
		if event.Type == eventType {
			count++
		}
	}
	return count
}
