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
	type candidate struct {
		member string
		score  float64
		raw    []byte
		node   sp.Node
	}
	var candidates []candidate
	min := "-inf"
	var cursorScore float64
	var offsetWithinScore int64
	hasCursor := false
	for scan := 0; scan < maxExpireScanRounds; scan++ {
		values, err := d.client.ZRangeByScoreWithScores(ctx, heartbeatKey, &goredis.ZRangeBy{
			Min:    min,
			Max:    strconv.FormatInt(cutoff, 10),
			Offset: offsetWithinScore,
			Count:  limit,
		}).Result()
		if err != nil {
			return 0, err
		}
		if len(values) == 0 {
			break
		}
		for _, value := range values {
			member, ok := value.Member.(string)
			if ok {
				candidates = append(candidates, candidate{member: member, score: value.Score})
			}
		}

		lastScore := values[len(values)-1].Score
		trailingAtLastScore := int64(0)
		for index := len(values) - 1; index >= 0 && values[index].Score == lastScore; index-- {
			trailingAtLastScore++
		}
		if hasCursor && lastScore == cursorScore {
			offsetWithinScore += trailingAtLastScore
		} else {
			cursorScore = lastScore
			offsetWithinScore = trailingAtLastScore
			hasCursor = true
		}
		min = strconv.FormatFloat(lastScore, 'f', -1, 64)
		if int64(len(values)) < limit {
			break
		}
	}

	for index := range candidates {
		raw, err := d.client.Get(ctx, candidates[index].member).Bytes()
		if err == goredis.Nil {
			continue
		}
		if err != nil {
			return 0, err
		}
		var node sp.Node
		if err := json.Unmarshal(raw, &node); err != nil {
			return 0, err
		}
		candidates[index].raw = raw
		candidates[index].node = node
	}

	expired := 0
	for _, candidate := range candidates {
		if int64(expired) == limit {
			break
		}
		result, err := d.client.Eval(ctx, expireHeartbeatLua, []string{
			candidate.member,
			heartbeatKey,
			EventsStreamKey(),
		},
			candidate.member,
			strconv.FormatFloat(candidate.score, 'f', -1, 64),
			strconv.FormatInt(cutoff, 10),
			string(candidate.raw),
			candidate.node.NodeSessionID,
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
