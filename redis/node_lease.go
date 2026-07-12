package redis

import (
	"context"
	"encoding/json"
	"fmt"
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
	expired := 0
	for _, candidate := range candidates {
		nodeKey, ok := candidate.Member.(string)
		if !ok {
			return expired, fmt.Errorf("invalid node lease member %T", candidate.Member)
		}
		raw, err := d.client.Get(ctx, nodeKey).Bytes()
		if err == goredis.Nil {
			raw = nil
		} else if err != nil {
			return expired, err
		}
		var node redisNode
		if len(raw) != 0 {
			if err := json.Unmarshal(raw, &node); err != nil {
				return expired, err
			}
		}
		result, err := d.client.Eval(ctx, expireNodeLeaseLua, []string{nodeKey, leaseKey, EventsStreamKey()}, nodeKey, string(raw), strconv.FormatFloat(candidate.Score, 'f', -1, 64), strconv.FormatInt(node.Lease.Version, 10), node.NodeSessionID, string(sp.EventNodeLeaseExpired)).Text()
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
