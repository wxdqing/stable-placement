//go:build integration

package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/redis"
)

const defaultRedisAddr = "127.0.0.1:6379"

type suite struct {
	t      *testing.T
	ctx    context.Context
	client *goredis.Client
	dir    *redis.Directory
	runID  string

	nodeType  string
	nodeGroup string

	nodes []sp.Node
}

func newSuite(t *testing.T) *suite {
	return newSuiteWithNodeLeaseConfig(t, sp.DefaultNodeLeaseConfig())
}

func newSuiteWithNodeLeaseConfig(t *testing.T, config sp.NodeLeaseConfig) *suite {
	t.Helper()
	addr := os.Getenv("STABLE_PLACEMENT_REDIS_ADDR")
	if addr == "" {
		addr = defaultRedisAddr
	}
	client := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: os.Getenv("STABLE_PLACEMENT_REDIS_PASSWORD"),
	})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis %s not available: %v", addr, err)
	}
	dir, err := redis.NewDirectory(client, sp.StrategyModeRedisRoundRobin, config)
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
	seen := make(map[string]struct{}, len(s.nodes))
	for _, node := range s.nodes {
		if _, ok := seen[node.NodeIdentity]; ok {
			continue
		}
		seen[node.NodeIdentity] = struct{}{}
		placements, err := s.placementsOn(node.NodeIdentity)
		if err != nil {
			s.t.Errorf("cleanup FindByNode %s failed: %v", node.NodeIdentity, err)
			continue
		}
		for _, placement := range placements {
			if err := s.releasePlacement(placement); err != nil {
				s.t.Errorf("cleanup Release %s failed: %v", placement.GrainKey, err)
			}
		}
	}
	for _, node := range s.nodes {
		_ = s.dir.UnregisterNode(s.ctx, node.NodeIdentity, node.NodeSessionID)
		_ = s.dir.RestoreNode(s.ctx, node.NodeType, node.NodeGroup, node.NodeName)
	}
	_ = s.client.Close()
}

func (s *suite) placementsOn(nodeIdentity string) ([]sp.Placement, error) {
	var placements []sp.Placement
	cursor := ""
	for {
		page, err := s.dir.FindByNode(s.ctx, sp.FindByNodeQuery{
			NodeIdentity: nodeIdentity,
			Cursor:       cursor,
			Limit:        50,
		})
		if err != nil {
			return nil, err
		}
		placements = append(placements, page.Placements...)
		if page.NextCursor == "" {
			return placements, nil
		}
		cursor = page.NextCursor
	}
}

func (s *suite) releasePlacement(placement sp.Placement) error {
	err := s.dir.Release(s.ctx, sp.ReleaseCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NodeIdentity: placement.NodeIdentity, NodeSessionID: placement.OwnerNodeSessionID, PlacementVersion: placement.Version})
	if err == nil {
		return nil
	}
	for _, node := range s.nodes {
		recovered, recoverErr := s.dir.Recover(s.ctx, sp.RecoverCommand{GrainKey: placement.GrainKey, PlacementID: placement.PlacementID, NewNodeIdentity: node.NodeIdentity, PlacementVersion: placement.Version})
		if recoverErr != nil {
			continue
		}
		return s.dir.Release(s.ctx, sp.ReleaseCommand{GrainKey: recovered.GrainKey, PlacementID: recovered.PlacementID, NodeIdentity: recovered.NodeIdentity, NodeSessionID: recovered.OwnerNodeSessionID, PlacementVersion: recovered.Version})
	}
	return err
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
	if _, err := s.dir.RegisterNode(s.ctx, node); err != nil {
		s.t.Fatalf("RegisterNode %s error: %v", name, err)
	}
	s.nodes = append(s.nodes, node)
	return node
}

func (s *suite) grainID(suffix string) string {
	return fmt.Sprintf("%s-%s", s.runID, suffix)
}

func (s *suite) allocate(grainID string) *sp.Placement {
	s.t.Helper()
	placement, err := s.dir.Allocate(s.ctx, sp.AllocateCommand{
		GrainID:         grainID,
		Kind:            "Player",
		TargetNodeType:  s.nodeType,
		TargetNodeGroup: s.nodeGroup,
	})
	if err != nil {
		s.t.Fatalf("Allocate %s error: %v", grainID, err)
	}
	return placement
}

func (s *suite) dirNode(identity string) sp.Node {
	s.t.Helper()
	nodes, err := s.dir.FindNodes(s.ctx, s.nodeType, s.nodeGroup)
	if err != nil {
		s.t.Fatalf("FindNodes error: %v", err)
	}
	for _, node := range nodes {
		if node.NodeIdentity == identity {
			return node
		}
	}
	s.t.Fatalf("node %s not found", identity)
	return sp.Node{}
}

func (s *suite) waitForLookupError(key sp.GrainKey, target error) {
	s.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, err := s.dir.Lookup(s.ctx, key)
		if errors.Is(err, target) {
			return
		}
		if err != nil {
			s.t.Fatalf("Lookup %s: err=%v want=%v", key, err, target)
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.t.Fatalf("Lookup %s did not return %v before deadline", key, target)
}
