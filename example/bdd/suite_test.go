//go:build integration

package bdd_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/redis"
)

const defaultRedisAddr = "127.0.0.1:16379"

type suite struct {
	t      *testing.T
	ctx    context.Context
	client *goredis.Client
	dir    *redis.Directory
	runID  string

	nodeType  string
	nodeGroup string

	nodes    []sp.Node
	grainIDs []string
}

func newSuite(t *testing.T) *suite {
	t.Helper()
	addr := os.Getenv("STABLE_PLACEMENT_REDIS_ADDR")
	if addr == "" {
		addr = defaultRedisAddr
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis %s not available: %v", addr, err)
	}
	dir, err := redis.NewDirectory(client, sp.StrategyModeRedisRoundRobin)
	if err != nil {
		t.Fatalf("NewDirectory error: %v", err)
	}
	runID := fmt.Sprintf("%x", time.Now().UnixNano())
	return &suite{
		t:         t,
		ctx:       ctx,
		client:    client,
		dir:       dir,
		runID:     runID,
		nodeType:  "game",
		nodeGroup: "bdd-" + runID,
	}
}

func (s *suite) cleanup() {
	for _, grainID := range s.grainIDs {
		key, err := sp.NewGrainKey("Player", grainID)
		if err != nil {
			continue
		}
		placement, err := s.dir.Lookup(s.ctx, key)
		if err != nil {
			continue
		}
		_ = s.dir.Release(s.ctx, sp.ReleaseCommand{
			GrainKey:         key,
			NodeIdentity:     placement.NodeIdentity,
			NodeSessionID:    placement.Lease.OwnerNodeSessionID,
			PlacementVersion: placement.Version,
			LeaseVersion:     placement.Lease.Version,
		})
	}
	for _, node := range s.nodes {
		_ = s.dir.UnregisterNode(s.ctx, node.NodeIdentity, node.NodeSessionID)
		_ = s.dir.RestoreNode(s.ctx, node.NodeType, node.NodeGroup, node.NodeName)
	}
	_ = s.client.Close()
}

func (s *suite) scenario(name string) {
	s.t.Helper()
	s.t.Logf("\n## %s", name)
}

func (s *suite) step(format string, args ...any) {
	s.t.Helper()
	s.t.Logf("  -> "+format, args...)
}

func (s *suite) registerNode(name string, session string) sp.Node {
	s.t.Helper()
	identity, err := sp.NewNodeIdentity(s.nodeType, s.nodeGroup, name)
	if err != nil {
		s.t.Fatal(err)
	}
	node := sp.Node{
		NodeType:      s.nodeType,
		NodeGroup:     s.nodeGroup,
		NodeName:      name,
		NodeIdentity:  identity.String(),
		NodeSessionID: session,
		Status:        sp.NodeStatusActive,
	}
	if err := s.dir.RegisterNode(s.ctx, node); err != nil {
		s.t.Fatalf("RegisterNode %s error: %v", name, err)
	}
	s.nodes = append(s.nodes, node)
	return node
}

func (s *suite) grainID(suffix string) string {
	id := fmt.Sprintf("%s-%s", s.runID, suffix)
	s.grainIDs = append(s.grainIDs, id)
	return id
}

func (s *suite) allocate(grainID string) *sp.Placement {
	s.t.Helper()
	placement, err := s.dir.Allocate(s.ctx, sp.AllocateCommand{
		GrainID:         grainID,
		Kind:            "Player",
		TargetNodeType:  s.nodeType,
		TargetNodeGroup: s.nodeGroup,
		LeaseTTL:        time.Minute,
	})
	if err != nil {
		s.t.Fatalf("Allocate %s error: %v", grainID, err)
	}
	return placement
}
