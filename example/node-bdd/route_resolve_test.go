//go:build integration

package nodebdd_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/memory"
	spredis "github.com/wxdqing/stable-placement/redis"
	"github.com/wxdqing/stable-placement/strategies"
)

const routeTestTTL = 150 * time.Millisecond

type routeTestBackend struct {
	directory sp.Directory
	registry  sp.NodeRegistry
	cleanup   func(sp.GrainKey, string)
}

func TestResolveRouteOwnerLifecycle(t *testing.T) {
	backends := []struct {
		name string
		new  func(*testing.T) routeTestBackend
	}{
		{name: "memory", new: newMemoryRouteBackend},
		{name: "redis", new: newRedisRouteBackend},
	}

	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			t.Run("B1_first_request_allocates", func(t *testing.T) {
				b, group := backend.new(t), routeTestGroup(t)
				node := registerRouteNode(t, b, group, "game-1", "session-1")
				route := mustResolveRoute(t, b, "account-1", group)
				assertRouteOwner(t, route, node, 1)
				releaseRoute(t, b, route)
			})

			t.Run("B2_concurrent_requests_have_one_owner", func(t *testing.T) {
				b, group := backend.new(t), routeTestGroup(t)
				registerRouteNode(t, b, group, "game-1", "session-1")
				registerRouteNode(t, b, group, "game-2", "session-2")
				const callers = 100
				routes := make(chan *sp.PlacementRoute, callers)
				errs := make(chan error, callers)
				var wg sync.WaitGroup
				for range callers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						route, err := b.directory.ResolveRoute(context.Background(), routeCommand("account-concurrent", group))
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
				var owner *sp.PlacementRoute
				for route := range routes {
					if owner == nil {
						owner = route
						continue
					}
					if !sameRoute(owner, route) {
						t.Fatalf("route=%+v owner=%+v", route, owner)
					}
				}
				releaseRoute(t, b, owner)
			})

			t.Run("B3_scale_out_keeps_existing_owner", func(t *testing.T) {
				b, group := backend.new(t), routeTestGroup(t)
				firstNode := registerRouteNode(t, b, group, "game-1", "session-1")
				first := mustResolveRoute(t, b, "account-existing", group)
				assertRouteOwner(t, first, firstNode, 1)
				secondNode := registerRouteNode(t, b, group, "game-2", "session-2")
				existing := mustResolveRoute(t, b, "account-existing", group)
				if !sameRoute(first, existing) {
					t.Fatalf("existing route changed: first=%+v existing=%+v", first, existing)
				}
				newRoute := mustResolveRoute(t, b, "account-new", group)
				assertRouteOwner(t, newRoute, secondNode, 1)
				releaseRoute(t, b, first)
				releaseRoute(t, b, newRoute)
			})

			t.Run("B4_same_identity_new_session_recovers", func(t *testing.T) {
				b, group := backend.new(t), routeTestGroup(t)
				node := registerRouteNode(t, b, group, "game-1", "session-1")
				first := mustResolveRoute(t, b, "account-restart", group)
				replacement := node
				replacement.NodeSessionID = "session-2"
				if _, _, err := b.registry.ReplaceNodeSession(context.Background(), replacement); err != nil {
					t.Fatalf("ReplaceNodeSession: %v", err)
				}
				t.Cleanup(func() {
					_ = b.registry.UnregisterNode(context.Background(), replacement.NodeIdentity, replacement.NodeSessionID)
				})
				recovered := mustResolveRoute(t, b, "account-restart", group)
				assertRouteOwner(t, recovered, replacement, first.Version+1)
				releaseRoute(t, b, recovered)
			})

			t.Run("B5_offline_owner_without_invalid_is_retained", func(t *testing.T) {
				b, group := backend.new(t), routeTestGroup(t)
				owner := registerRouteNode(t, b, group, "game-1", "session-1")
				first := mustResolveRoute(t, b, "account-offline", group)
				expireRouteNode(t, b, group)
				route, err := b.directory.ResolveRoute(context.Background(), routeCommand("account-offline", group))
				if route != nil || !errors.Is(err, sp.ErrPlacementOwnerUnavailable) {
					t.Fatalf("route=%+v err=%v", route, err)
				}
				if err := b.registry.MarkNodeInvalid(context.Background(), owner.NodeType, owner.NodeGroup, owner.NodeName); err != nil {
					t.Fatalf("MarkNodeInvalid cleanup: %v", err)
				}
				registerRouteNode(t, b, group, "game-2", "session-2")
				recovered := mustResolveRoute(t, b, "account-offline", group)
				if recovered.Version != first.Version+1 {
					t.Fatalf("recovered version=%d want=%d", recovered.Version, first.Version+1)
				}
				releaseRoute(t, b, recovered)
			})

			t.Run("B6_manual_invalid_allows_cross_node_recover", func(t *testing.T) {
				b, group := backend.new(t), routeTestGroup(t)
				owner := registerRouteNode(t, b, group, "game-1", "session-1")
				first := mustResolveRoute(t, b, "account-invalid", group)
				expireRouteNode(t, b, group)
				if err := b.registry.MarkNodeInvalid(context.Background(), owner.NodeType, owner.NodeGroup, owner.NodeName); err != nil {
					t.Fatalf("MarkNodeInvalid: %v", err)
				}
				target := registerRouteNode(t, b, group, "game-2", "session-2")
				recovered := mustResolveRoute(t, b, "account-invalid", group)
				assertRouteOwner(t, recovered, target, first.Version+1)
				releaseRoute(t, b, recovered)
			})

			t.Run("B7_healthy_invalid_requires_explicit_transfer", func(t *testing.T) {
				b, group := backend.new(t), routeTestGroup(t)
				owner := registerRouteNode(t, b, group, "game-1", "session-1")
				target := registerRouteNode(t, b, group, "game-2", "session-2")
				first := mustResolveRoute(t, b, "account-transfer", group)
				if err := b.registry.MarkNodeInvalid(context.Background(), owner.NodeType, owner.NodeGroup, owner.NodeName); err != nil {
					t.Fatalf("MarkNodeInvalid: %v", err)
				}
				retained := mustResolveRoute(t, b, "account-transfer", group)
				if !sameRoute(first, retained) {
					t.Fatalf("healthy invalid owner changed: first=%+v retained=%+v", first, retained)
				}
				transferred, err := b.directory.Transfer(context.Background(), sp.TransferCommand{
					GrainKey: first.GrainKey, FromNodeIdentity: owner.NodeIdentity,
					ToNodeIdentity: target.NodeIdentity, PlacementVersion: first.Version,
				})
				if err != nil {
					t.Fatalf("Transfer: %v", err)
				}
				if transferred.NodeIdentity != target.NodeIdentity || transferred.OwnerNodeSessionID != target.NodeSessionID || transferred.Version != first.Version+1 {
					t.Fatalf("transferred=%+v", transferred)
				}
				releasePlacement(t, b, transferred)
			})

			t.Run("B8_target_group_change_is_rejected", func(t *testing.T) {
				b, group := backend.new(t), routeTestGroup(t)
				registerRouteNode(t, b, group, "game-1", "session-1")
				first := mustResolveRoute(t, b, "account-mismatch", group)
				route, err := b.directory.ResolveRoute(context.Background(), routeCommand("account-mismatch", group+"-other"))
				if route != nil || !errors.Is(err, sp.ErrPlacementTargetMismatch) {
					t.Fatalf("route=%+v err=%v", route, err)
				}
				releaseRoute(t, b, first)
			})
		})
	}
}

func newMemoryRouteBackend(t *testing.T) routeTestBackend {
	t.Helper()
	registry, err := memory.NewNodeRegistry(nil, sp.NodeLeaseConfig{TTL: routeTestTTL})
	if err != nil {
		t.Fatal(err)
	}
	directory, err := memory.NewDirectory(registry, sp.StrategyModeGo, strategies.NewRoundRobin(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return routeTestBackend{directory: directory, registry: registry}
}

func newRedisRouteBackend(t *testing.T) routeTestBackend {
	t.Helper()
	address := os.Getenv("STABLE_PLACEMENT_REDIS_ADDR")
	if address == "" {
		address = defaultRedisAddr
	}
	client := goredis.NewClient(&goredis.Options{Addr: address, Password: os.Getenv("STABLE_PLACEMENT_REDIS_PASSWORD")})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis %s unavailable: %v", address, err)
	}
	directory, err := spredis.NewDirectory(client, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: routeTestTTL})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close Redis: %v", err)
		}
	})
	return routeTestBackend{
		directory: directory,
		registry:  directory,
		cleanup: func(key sp.GrainKey, group string) {
			if err := client.Del(context.Background(), spredis.PlacementKey(key), spredis.StrategyRoundRobinKey("game", group)).Err(); err != nil {
				t.Errorf("cleanup Redis route: %v", err)
			}
		},
	}
}

func routeTestGroup(t *testing.T) string {
	t.Helper()
	name := strings.NewReplacer("/", "-", "_", "-").Replace(t.Name())
	return fmt.Sprintf("bdd-%s-%x", name, time.Now().UnixNano())
}

func registerRouteNode(t *testing.T, b routeTestBackend, group, name, session string) sp.Node {
	t.Helper()
	identity, err := sp.NewNodeIdentity("game", group, name)
	if err != nil {
		t.Fatal(err)
	}
	node := sp.Node{NodeType: "game", NodeGroup: group, NodeName: name, NodeIdentity: identity.String(), NodeSessionID: session}
	if _, err := b.registry.RegisterNode(context.Background(), node); err != nil {
		t.Fatalf("RegisterNode %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = b.registry.RestoreNode(context.Background(), node.NodeType, node.NodeGroup, node.NodeName)
		if err := b.registry.UnregisterNode(context.Background(), node.NodeIdentity, node.NodeSessionID); err != nil &&
			!errors.Is(err, sp.ErrNodeNotFound) && !errors.Is(err, sp.ErrInvalidNodeSession) {
			t.Errorf("UnregisterNode %s: %v", node.NodeIdentity, err)
		}
	})
	return node
}

func routeCommand(grainID, group string) sp.ResolveRouteCommand {
	return sp.ResolveRouteCommand{GrainID: grainID, Kind: "player", TargetNodeType: "game", TargetNodeGroup: group}
}

func mustResolveRoute(t *testing.T, b routeTestBackend, grainID, group string) *sp.PlacementRoute {
	t.Helper()
	route, err := b.directory.ResolveRoute(context.Background(), routeCommand(grainID, group))
	if err != nil {
		t.Fatalf("ResolveRoute %s: %v", grainID, err)
	}
	if b.cleanup != nil {
		t.Cleanup(func() { b.cleanup(route.GrainKey, group) })
	}
	return route
}

func expireRouteNode(t *testing.T, b routeTestBackend, group string) {
	t.Helper()
	time.Sleep(routeTestTTL + 75*time.Millisecond)
	count, err := b.registry.ExpireNodeLeases(context.Background(), "game", group, 10)
	if err != nil || count != 1 {
		t.Fatalf("ExpireNodeLeases count=%d err=%v", count, err)
	}
}

func assertRouteOwner(t *testing.T, route *sp.PlacementRoute, node sp.Node, version int64) {
	t.Helper()
	if route.NodeIdentity != node.NodeIdentity || route.OwnerNodeSessionID != node.NodeSessionID || route.Version != version {
		t.Fatalf("route=%+v want identity=%s session=%s version=%d", route, node.NodeIdentity, node.NodeSessionID, version)
	}
}

func sameRoute(left, right *sp.PlacementRoute) bool {
	return left != nil && right != nil && left.GrainKey == right.GrainKey && left.NodeIdentity == right.NodeIdentity &&
		left.OwnerNodeSessionID == right.OwnerNodeSessionID && left.Version == right.Version && left.Status == right.Status
}

func releaseRoute(t *testing.T, b routeTestBackend, route *sp.PlacementRoute) {
	t.Helper()
	releasePlacement(t, b, &sp.Placement{
		GrainKey: route.GrainKey, NodeIdentity: route.NodeIdentity, OwnerNodeSessionID: route.OwnerNodeSessionID,
		Version: route.Version,
	})
}

func releasePlacement(t *testing.T, b routeTestBackend, placement *sp.Placement) {
	t.Helper()
	if err := b.directory.Release(context.Background(), sp.ReleaseCommand{
		GrainKey: placement.GrainKey, NodeIdentity: placement.NodeIdentity,
		NodeSessionID: placement.OwnerNodeSessionID, PlacementVersion: placement.Version,
	}); err != nil {
		t.Fatalf("Release %s: %v", placement.GrainKey, err)
	}
}
