package redis

import (
	"context"
	"errors"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

func (d *Directory) ExpireDue(ctx context.Context, now time.Time, limit int64) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	keys, err := d.client.ZRangeByScore(ctx, LeaseExpireKey(), &goredis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(now.UnixMilli(), 10),
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return 0, err
	}

	expired := 0
	for _, rawKey := range keys {
		placement, err := d.getPlacement(ctx, sp.GrainKey(rawKey))
		if err == nil {
			err = d.Expire(ctx, sp.ExpireCommand{
				GrainKey:     placement.GrainKey,
				LeaseVersion: placement.Lease.Version,
				Now:          now,
			})
		}
		if err == nil {
			expired++
			continue
		}
		if errors.Is(err, sp.ErrPlacementNotFound) ||
			errors.Is(err, sp.ErrVersionConflict) ||
			errors.Is(err, sp.ErrLeaseNotExpired) {
			continue
		}
		return expired, err
	}
	return expired, nil
}

func (d *Directory) RunExpireLoop(ctx context.Context, interval time.Duration, batchSize int64) error {
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
