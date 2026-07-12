package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func newTestDirectory(t *testing.T, config sp.NodeLeaseConfig) (*Directory, *goredis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	directory, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin, config)
	if err != nil {
		t.Fatal(err)
	}
	return directory, client, server
}

func testNode(name, session string) sp.Node {
	return sp.Node{NodeType: "game", NodeGroup: "default", NodeName: name,
		NodeIdentity: "game/default/" + name, NodeSessionID: session, Address: name + ":8080", Weight: 1}
}

func TestRedisDirectoryImplementsContracts(t *testing.T) {
	var _ sp.Directory = (*Directory)(nil)
	var _ sp.NodeRegistry = (*Directory)(nil)
}

func TestRedisDirectoryNodeLeaseConfig(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	for _, tc := range []struct {
		name    string
		ttl     time.Duration
		wantErr error
	}{
		{"positive", time.Second, nil}, {"submillisecond", time.Nanosecond, nil},
		{"zero", 0, sp.ErrInvalidNodeLeaseTTL}, {"negative", -time.Second, sp.ErrInvalidNodeLeaseTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: tc.ttl})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
	if sp.DefaultNodeLeaseConfig().TTL != time.Minute {
		t.Fatal("default TTL must be one minute")
	}
	maxDir, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Duration(1<<63 - 1)})
	if err != nil || maxDir.config.TTL <= 0 || maxDir.config.TTL > time.Duration(1<<63-1) {
		t.Fatalf("maximum TTL normalized to %v, err %v", maxDir.config.TTL, err)
	}
}

func readNode(t *testing.T, ctx context.Context, directory *Directory, identity string) sp.Node {
	t.Helper()
	nodes, err := directory.FindNodes(ctx, "game", "default")
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range nodes {
		if node.NodeIdentity == identity {
			return node
		}
	}
	t.Fatalf("node %q not found", identity)
	return sp.Node{}
}
