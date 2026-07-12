package redis

import (
	"context"
	"errors"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

const cleanupStaleLeaseLua = `
local score = redis.call("ZSCORE", KEYS[2], ARGV[1])
if not score or tonumber(score) ~= tonumber(ARGV[2]) then
	return 0
end
local raw = redis.call("GET", KEYS[1])
if raw then
	local placement = cjson.decode(raw)
	if placement["Status"] == "active" then
		return 0
	end
end
return redis.call("ZREM", KEYS[2], ARGV[1])
`

const maxExpireScanRounds = 100

func (d *Directory) ExpireDue(ctx context.Context, now time.Time, limit int64) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	type candidate struct {
		key   sp.GrainKey
		score float64
	}
	var candidates []candidate
	min := "-inf"
	var cursorScore float64
	var offsetWithinScore int64
	hasCursor := false
	for scan := 0; scan < maxExpireScanRounds; scan++ {
		values, err := d.client.ZRangeByScoreWithScores(ctx, LeaseExpireKey(), &goredis.ZRangeBy{
			Min:    min,
			Max:    strconv.FormatInt(now.UnixMilli(), 10),
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
			rawKey, ok := value.Member.(string)
			if !ok {
				continue
			}
			candidates = append(candidates, candidate{key: sp.GrainKey(rawKey), score: value.Score})
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

	expired := 0
	for _, candidate := range candidates {
		if int64(expired) == limit {
			break
		}
		key := candidate.key
		placement, err := d.getPlacement(ctx, key)
		if errors.Is(err, sp.ErrPlacementNotFound) {
			_, cleanupErr := d.cleanupStaleLease(ctx, key, candidate.score)
			if cleanupErr != nil {
				return expired, cleanupErr
			}
			continue
		}
		if err != nil {
			return expired, err
		}
		if placement.Status != sp.PlacementStatusActive {
			_, cleanupErr := d.cleanupStaleLease(ctx, key, candidate.score)
			if cleanupErr != nil {
				return expired, cleanupErr
			}
			continue
		}

		err = d.Expire(ctx, sp.ExpireCommand{
			GrainKey:     placement.GrainKey,
			LeaseVersion: placement.Lease.Version,
			Now:          now,
		})
		if err == nil {
			expired++
			continue
		}
		if errors.Is(err, sp.ErrPlacementNotFound) {
			_, cleanupErr := d.cleanupStaleLease(ctx, key, candidate.score)
			if cleanupErr != nil {
				return expired, cleanupErr
			}
			continue
		}
		if errors.Is(err, sp.ErrVersionConflict) || errors.Is(err, sp.ErrLeaseNotExpired) {
			continue
		}
		return expired, err
	}
	return expired, nil
}

func (d *Directory) cleanupStaleLease(ctx context.Context, key sp.GrainKey, observedScore float64) (bool, error) {
	removed, err := d.client.Eval(ctx, cleanupStaleLeaseLua, []string{
		PlacementKey(key),
		LeaseExpireKey(),
	}, key.String(), strconv.FormatFloat(observedScore, 'f', -1, 64)).Int()
	return removed > 0, err
}

func (d *Directory) RunExpireLoop(ctx context.Context, interval time.Duration, batchSize int64) error {
	if ctx.Err() != nil {
		return nil
	}
	if interval <= 0 {
		_, err := d.ExpireDue(ctx, time.Now(), batchSize)
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
			if _, err := d.ExpireDue(ctx, now, batchSize); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}
