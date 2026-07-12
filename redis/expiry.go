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

func (d *Directory) ExpireDue(ctx context.Context, now time.Time, limit int64) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	expired := 0
	skipped := make(map[string]struct{})
	for scan := 0; scan < 100 && int64(expired) < limit; scan++ {
		count := limit - int64(expired) + int64(len(skipped))
		values, err := d.client.ZRangeByScoreWithScores(ctx, LeaseExpireKey(), &goredis.ZRangeBy{
			Min:    "-inf",
			Max:    strconv.FormatInt(now.UnixMilli(), 10),
			Offset: 0,
			Count:  count,
		}).Result()
		if err != nil {
			return expired, err
		}
		if len(values) == 0 {
			break
		}

		progressed := false
		for _, value := range values {
			rawKey, ok := value.Member.(string)
			if !ok {
				continue
			}
			if _, ok := skipped[rawKey]; ok {
				continue
			}
			key := sp.GrainKey(rawKey)
			placement, err := d.getPlacement(ctx, key)
			if errors.Is(err, sp.ErrPlacementNotFound) {
				removed, cleanupErr := d.cleanupStaleLease(ctx, key, value.Score)
				if cleanupErr != nil {
					return expired, cleanupErr
				}
				progressed = progressed || removed
				continue
			}
			if err != nil {
				return expired, err
			}
			if placement.Status != sp.PlacementStatusActive {
				removed, cleanupErr := d.cleanupStaleLease(ctx, key, value.Score)
				if cleanupErr != nil {
					return expired, cleanupErr
				}
				progressed = progressed || removed
				continue
			}

			err = d.Expire(ctx, sp.ExpireCommand{
				GrainKey:     placement.GrainKey,
				LeaseVersion: placement.Lease.Version,
				Now:          now,
			})
			if err == nil {
				expired++
				progressed = true
				continue
			}
			if errors.Is(err, sp.ErrPlacementNotFound) {
				removed, cleanupErr := d.cleanupStaleLease(ctx, key, value.Score)
				if cleanupErr != nil {
					return expired, cleanupErr
				}
				progressed = progressed || removed
				continue
			}
			if errors.Is(err, sp.ErrVersionConflict) || errors.Is(err, sp.ErrLeaseNotExpired) {
				skipped[rawKey] = struct{}{}
				continue
			}
			return expired, err
		}
		if !progressed {
			break
		}
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
