package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func (d *Directory) SetHeartbeatTTL(ttl time.Duration) {
	d.heartbeatMu.Lock()
	d.heartbeatTTL = ttl
	d.heartbeatMu.Unlock()
}

func (d *Directory) ExpireHeartbeats(ctx context.Context, nodeType, nodeGroup string, now time.Time, limit int64) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	d.heartbeatMu.RLock()
	ttl := d.heartbeatTTL
	d.heartbeatMu.RUnlock()
	cutoff := now.Add(-ttl).UnixMilli()
	heartbeatKey := NodeHeartbeatKey(nodeType, nodeGroup)
	values, err := d.client.ZRangeByScoreWithScores(ctx, heartbeatKey, &goredis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(cutoff, 10),
		Count: limit,
	}).Result()
	if err != nil {
		return 0, err
	}

	expired := 0
	for _, value := range values {
		member, ok := value.Member.(string)
		if !ok {
			continue
		}
		raw, err := d.client.Get(ctx, member).Bytes()
		if err == goredis.Nil {
			raw = nil
		} else if err != nil {
			return expired, err
		}
		var node sp.Node
		if raw != nil {
			if err := json.Unmarshal(raw, &node); err != nil {
				return expired, err
			}
		}
		result, err := d.client.Eval(ctx, expireHeartbeatLua, []string{
			member,
			heartbeatKey,
			EventsStreamKey(),
		},
			member,
			strconv.FormatFloat(value.Score, 'f', -1, 64),
			strconv.FormatInt(cutoff, 10),
			string(raw),
			node.NodeSessionID,
			nodeType,
			nodeGroup,
			string(sp.EventNodeUnregistered),
		).Int()
		if err != nil {
			return expired, err
		}
		expired += result
	}
	return expired, nil
}

func (d *Directory) RunHeartbeatLoop(ctx context.Context, nodeType, nodeGroup string, interval time.Duration, batchSize int64) error {
	if ctx.Err() != nil {
		return nil
	}
	if interval <= 0 {
		_, err := d.ExpireHeartbeats(ctx, nodeType, nodeGroup, time.Now(), batchSize)
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if _, err := d.ExpireHeartbeats(ctx, nodeType, nodeGroup, now, batchSize); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}
