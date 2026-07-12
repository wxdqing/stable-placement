package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func (d *Directory) ExpireNodeLeases(ctx context.Context, nodeType, nodeGroup string, limit int64) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	leaseKey := NodeLeaseKey(nodeType, nodeGroup)
	candidates, err := d.client.ZRangeByScoreWithScores(ctx, leaseKey, &goredis.ZRangeBy{Min: "-inf", Max: "+inf", Offset: 0, Count: limit}).Result()
	if err != nil {
		return 0, err
	}
	type candidateSnapshot struct {
		nodeKey string
		raw     []byte
		node    redisNode
		score   float64
	}
	snapshots := make([]candidateSnapshot, 0, len(candidates))
	streamType, err := d.client.Type(ctx, EventsStreamKey()).Result()
	if err != nil {
		return 0, err
	}
	if streamType != "none" && streamType != "stream" {
		return 0, fmt.Errorf("WRONGTYPE events expected stream got %s", streamType)
	}
	for _, candidate := range candidates {
		nodeKey, ok := candidate.Member.(string)
		if !ok {
			return 0, fmt.Errorf("invalid node lease member %T", candidate.Member)
		}
		if math.IsNaN(candidate.Score) || math.IsInf(candidate.Score, 0) || math.Trunc(candidate.Score) != candidate.Score {
			return 0, fmt.Errorf("invalid node lease score %v", candidate.Score)
		}
		raw, err := d.client.Get(ctx, nodeKey).Bytes()
		if err == goredis.Nil {
			raw = nil
		} else if err != nil {
			return 0, err
		}
		var node redisNode
		if len(raw) != 0 {
			if err := json.Unmarshal(raw, &node); err != nil {
				return 0, err
			}
			if node.NodeKey != nodeKey || node.Lease.Version <= 0 || node.Lease.TTLMillis <= 0 {
				return 0, fmt.Errorf("invalid node lease snapshot %q", nodeKey)
			}
			if node.Status != sp.NodeStatusActive && node.Status != sp.NodeStatusDraining && node.Status != sp.NodeStatusOffline {
				return 0, fmt.Errorf("invalid node status %q", node.Status)
			}
		}
		snapshots = append(snapshots, candidateSnapshot{nodeKey: nodeKey, raw: raw, node: node, score: candidate.Score})
	}
	expired := 0
	for _, candidate := range snapshots {
		result, err := d.client.Eval(ctx, expireNodeLeaseLua, []string{candidate.nodeKey, leaseKey, EventsStreamKey()}, candidate.nodeKey, string(candidate.raw), strconv.FormatFloat(candidate.score, 'f', -1, 64), strconv.FormatInt(candidate.node.Lease.Version, 10), candidate.node.NodeSessionID, string(sp.EventNodeLeaseExpired)).Text()
		if err != nil {
			return expired, err
		}
		if result == "expired" {
			expired++
		}
	}
	return expired, nil
}

func (d *Directory) RunNodeLeaseLoop(ctx context.Context, nodeType, nodeGroup string, interval time.Duration, batchSize int64) error {
	if interval <= 0 {
		return fmt.Errorf("node lease interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := d.ExpireNodeLeases(ctx, nodeType, nodeGroup, batchSize); err != nil {
				return err
			}
		}
	}
}
