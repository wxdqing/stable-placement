package redis

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type nodeLeaseEvalHookClient struct {
	goredis.UniversalClient
	before func(string)
}

func (c nodeLeaseEvalHookClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *goredis.Cmd {
	if c.before != nil {
		c.before(script)
	}
	return c.UniversalClient.Eval(ctx, script, keys, args...)
}

func TestRedisDirectoryRegisterAndRenewNodeLease(t *testing.T) {
	ctx := context.Background()
	dir, _, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: 1500 * time.Microsecond})
	server.SetTime(time.Unix(100, 0))
	node := testNode("game-1", "session-a")
	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	registered := readNode(t, ctx, dir, node.NodeIdentity)
	if registered.Status != sp.NodeStatusActive || registered.Lease.Version != 1 || registered.Lease.TTLMillis != 2 || registered.Lease.ExpireAtUnixMilli != 100002 {
		t.Fatalf("registered = %+v", registered)
	}
	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatalf("idempotent register: %v", err)
	}
	if got := readNode(t, ctx, dir, node.NodeIdentity); got.Lease != registered.Lease {
		t.Fatalf("register renewed lease: %+v", got.Lease)
	}
	different := node
	different.NodeSessionID = "session-b"
	if err := dir.RegisterNode(ctx, different); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("different session err = %v", err)
	}
	server.SetTime(time.UnixMilli(100001))
	if err := dir.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatal(err)
	}
	renewed := readNode(t, ctx, dir, node.NodeIdentity)
	if renewed.Lease.Version != 2 || renewed.Lease.TTLMillis != 2 || renewed.Lease.ExpireAtUnixMilli != 100003 {
		t.Fatalf("renewed = %+v", renewed)
	}
}

func TestRedisDirectoryRenewUsesPersistedTTL(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: 5 * time.Second})
	server.SetTime(time.Unix(200, 0))
	node := testNode("game-1", "session-a")
	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	other, err := NewDirectory(client, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.UnixMilli(201000))
	if err := other.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
		t.Fatal(err)
	}
	got := readNode(t, ctx, other, node.NodeIdentity)
	if got.Lease.TTLMillis != 5000 || got.Lease.ExpireAtUnixMilli != 206000 {
		t.Fatalf("lease = %+v", got.Lease)
	}
}

func TestRedisDirectoryRenewRejectsExpiredOfflineAndOldSession(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(300, 0))
	node := testNode("game-1", "session-a")
	if err := dir.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	if err := dir.RenewNode(ctx, node.NodeIdentity, "old"); !errors.Is(err, sp.ErrInvalidNodeSession) {
		t.Fatalf("old session: %v", err)
	}
	server.SetTime(time.Unix(301, 0))
	if err := dir.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeLeaseExpired) {
		t.Fatalf("expired: %v", err)
	}
	raw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
	var stored redisNode
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatal(err)
	}
	stored.Status = sp.NodeStatusOffline
	b, _ := json.Marshal(stored)
	client.Set(ctx, NodeKey(node.NodeIdentity), b, 0)
	if err := dir.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); !errors.Is(err, sp.ErrNodeNotFound) {
		t.Fatalf("offline: %v", err)
	}
}

func TestExpireNodeLeasesUsesRedisTimeAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(400, 0))
	for _, node := range []sp.Node{testNode("game-1", "session-a"), testNode("game-2", "session-b")} {
		if err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	server.SetTime(time.Unix(401, 0))
	count, err := dir.ExpireNodeLeases(ctx, "game", "default", 1)
	if err != nil || count != 1 {
		t.Fatalf("first scan = %d, %v", count, err)
	}
	count, err = dir.ExpireNodeLeases(ctx, "game", "default", 10)
	if err != nil || count != 1 {
		t.Fatalf("second scan = %d, %v", count, err)
	}
	count, err = dir.ExpireNodeLeases(ctx, "game", "default", 10)
	if err != nil || count != 0 {
		t.Fatalf("repeat scan = %d, %v", count, err)
	}
	events := client.XRange(ctx, EventsStreamKey(), "-", "+").Val()
	expired := 0
	for _, event := range events {
		if event.Values["type"] == string(sp.EventNodeLeaseExpired) {
			expired++
		}
	}
	if expired != 2 {
		t.Fatalf("expired events = %d", expired)
	}
}

func TestRedisDirectoryReplaceNodeSessionPreflightsBeforeWrite(t *testing.T) {
	ctx := context.Background()
	t.Run("same session", func(t *testing.T) {
		dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
		server.SetTime(time.Unix(800, 0))
		node := testNode("game-1", "session-a")
		if err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
		raw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
		score := client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val()
		events := client.XLen(ctx, EventsStreamKey()).Val()
		if _, err := dir.ReplaceNodeSession(ctx, node); !errors.Is(err, sp.ErrInvalidNodeSession) {
			t.Fatalf("err = %v", err)
		}
		if client.Get(ctx, NodeKey(node.NodeIdentity)).Val() != raw || client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val() != score || client.XLen(ctx, EventsStreamKey()).Val() != events {
			t.Fatal("same-session replace changed state")
		}
	})
	t.Run("malformed old json", func(t *testing.T) {
		dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
		server.SetTime(time.Unix(810, 0))
		node := testNode("game-1", "session-a")
		if err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
		client.Set(ctx, NodeKey(node.NodeIdentity), "{", 0)
		score := client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val()
		events := client.XLen(ctx, EventsStreamKey()).Val()
		next := node
		next.NodeSessionID = "session-b"
		if _, err := dir.ReplaceNodeSession(ctx, next); err == nil {
			t.Fatal("expected malformed JSON error")
		}
		if client.Get(ctx, NodeKey(node.NodeIdentity)).Val() != "{" || client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val() != score || client.XLen(ctx, EventsStreamKey()).Val() != events {
			t.Fatal("malformed replace changed state")
		}
	})
	t.Run("events wrong type", func(t *testing.T) {
		dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
		server.SetTime(time.Unix(820, 0))
		node := testNode("game-1", "session-a")
		if err := dir.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
		raw := client.Get(ctx, NodeKey(node.NodeIdentity)).Val()
		score := client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val()
		client.Del(ctx, EventsStreamKey())
		client.Set(ctx, EventsStreamKey(), "wrongtype", 0)
		next := node
		next.NodeSessionID = "session-b"
		if _, err := dir.ReplaceNodeSession(ctx, next); err == nil {
			t.Fatal("expected WRONGTYPE")
		}
		if client.Get(ctx, NodeKey(node.NodeIdentity)).Val() != raw || client.ZScore(ctx, NodeLeaseKey("game", "default"), NodeKey(node.NodeIdentity)).Val() != score || client.Get(ctx, EventsStreamKey()).Val() != "wrongtype" {
			t.Fatal("WRONGTYPE replace changed state")
		}
	})
}

func TestRedisDirectoryReplaceNodeSessionWritesSingleEvent(t *testing.T) {
	ctx := context.Background()
	dir, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(830, 0))
	old := testNode("game-1", "session-a")
	if err := dir.RegisterNode(ctx, old); err != nil {
		t.Fatal(err)
	}
	next := old
	next.NodeSessionID = "session-b"
	returned, err := dir.ReplaceNodeSession(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	if returned.NodeSessionID != "session-a" {
		t.Fatalf("old = %+v", returned)
	}
	events := client.XRange(ctx, EventsStreamKey(), "-", "+").Val()
	replaced := 0
	for _, event := range events {
		if event.Values["type"] == string(sp.EventNodeReplaced) {
			replaced++
		}
	}
	if replaced != 1 {
		t.Fatalf("replace events = %d", replaced)
	}
}

func TestExpireNodeLeasesStaleCandidateDoesNotExpireRenewedLease(t *testing.T) {
	ctx := context.Background()
	base, client, server := newTestDirectory(t, sp.NodeLeaseConfig{TTL: time.Second})
	server.SetTime(time.Unix(840, 0))
	node := testNode("game-1", "session-a")
	if err := base.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.UnixMilli(840500))
	armed := true
	hooked, err := NewDirectory(nodeLeaseEvalHookClient{UniversalClient: client, before: func(script string) {
		if armed && script == expireNodeLeaseLua {
			armed = false
			if err := base.RenewNode(ctx, node.NodeIdentity, node.NodeSessionID); err != nil {
				t.Fatalf("concurrent renew: %v", err)
			}
		}
	}}, sp.StrategyModeRedisRoundRobin, sp.NodeLeaseConfig{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	count, err := hooked.ExpireNodeLeases(ctx, "game", "default", 1)
	if err != nil || count != 0 {
		t.Fatalf("scan = %d, %v", count, err)
	}
	got := readNode(t, ctx, base, node.NodeIdentity)
	if got.Status != sp.NodeStatusActive || got.Lease.Version != 2 || got.Lease.ExpireAtUnixMilli != 841500 {
		t.Fatalf("renewed node = %+v", got)
	}
	for _, event := range client.XRange(ctx, EventsStreamKey(), "-", "+").Val() {
		if event.Values["type"] == string(sp.EventNodeLeaseExpired) {
			t.Fatal("stale scan wrote expiry event")
		}
	}
}
