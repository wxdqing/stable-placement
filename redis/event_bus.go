package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type EventBus struct {
	client   goredis.UniversalClient
	stream   string
	consumer StreamConsumer

	mu       sync.RWMutex
	degraded bool
}

func NewEventBus(client goredis.UniversalClient, consumer StreamConsumer) *EventBus {
	return &EventBus{
		client:   client,
		stream:   EventsStreamKey(),
		consumer: consumer,
	}
}

func (b *EventBus) StreamKey() string {
	return b.stream
}

func (b *EventBus) IsDegraded() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.degraded
}

func (b *EventBus) Publish(ctx context.Context, event sp.PlacementEvent) error {
	return b.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: b.stream,
		Values: eventValues(event),
	}).Err()
}

// PublishHint sends an optional low-latency notification without writing the reliable Stream outbox.
func (b *EventBus) PublishHint(ctx context.Context, event sp.PlacementEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, EventsPubSubChannelKey(), payload).Err()
}

// SubscribeHint consumes optional Pub/Sub notifications; callers must still rely on Stream for durability.
func (b *EventBus) SubscribeHint(ctx context.Context, handler func(sp.PlacementEvent) error) error {
	pubsub := b.client.Subscribe(ctx, EventsPubSubChannelKey())
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		b.setDegraded()
		return err
	}
	for {
		message, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			b.setDegraded()
			return err
		}
		var event sp.PlacementEvent
		if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
			b.setDegraded()
			return err
		}
		if err := handler(event); err != nil {
			b.setDegraded()
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (b *EventBus) EnsureConsumerGroup(ctx context.Context) error {
	if b.consumer.Group != ConsumerGroupName(b.consumer.NodeIdentity, b.consumer.NodeSessionID) {
		b.setDegraded()
		return ErrSharedConsumerGroup
	}
	err := b.client.XGroupCreateMkStream(ctx, b.stream, b.consumer.Group, "$").Err()
	if err == nil || stringsHasBusyGroup(err) {
		return nil
	}
	return err
}

func (b *EventBus) DeleteConsumerGroup(ctx context.Context) error {
	err := b.client.XGroupDestroy(ctx, b.stream, b.consumer.Group).Err()
	if err == goredis.Nil {
		return nil
	}
	return err
}

// CleanupConsumerGroup removes a previously valid node-session consumer group.
func (b *EventBus) CleanupConsumerGroup(ctx context.Context, consumer StreamConsumer) error {
	if consumer.Group != ConsumerGroupName(consumer.NodeIdentity, consumer.NodeSessionID) {
		b.setDegraded()
		return ErrSharedConsumerGroup
	}
	err := b.client.XGroupDestroy(ctx, b.stream, consumer.Group).Err()
	if err == goredis.Nil {
		return nil
	}
	return err
}

// CheckPending enters degraded mode when this consumer group has messages idle beyond threshold.
func (b *EventBus) CheckPending(ctx context.Context, threshold time.Duration) error {
	pending, err := b.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: b.stream,
		Group:  b.consumer.Group,
		Idle:   threshold,
		Start:  "-",
		End:    "+",
		Count:  1,
	}).Result()
	if err != nil {
		b.setDegraded()
		return err
	}
	if len(pending) > 0 {
		b.setDegraded()
		return ErrPendingMessages
	}
	return nil
}

// Trim shortens the Stream only when no consumer group has pending messages.
func (b *EventBus) Trim(ctx context.Context, maxLen int64) error {
	groups, err := b.client.XInfoGroups(ctx, b.stream).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return nil
		}
		return err
	}
	for _, group := range groups {
		if group.Pending > 0 {
			return nil
		}
	}
	return b.client.XTrimMaxLen(ctx, b.stream, maxLen).Err()
}

// RunTrimLoop periodically applies the same pending-safe trim policy as Trim.
func (b *EventBus) RunTrimLoop(ctx context.Context, interval time.Duration, maxLen int64) error {
	if interval <= 0 {
		return b.Trim(ctx, maxLen)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if ctx.Err() != nil {
				return nil
			}
			if err := b.Trim(ctx, maxLen); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				b.setDegraded()
				return err
			}
		}
	}
}

// CheckContinuity enters degraded mode when Redis reports that this group may have missed trimmed events.
func (b *EventBus) CheckContinuity(ctx context.Context) error {
	info, err := b.client.XInfoStream(ctx, b.stream).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return nil
		}
		b.setDegraded()
		return err
	}
	trimmed := info.MaxDeletedEntryID != "" && info.MaxDeletedEntryID != "0-0"
	trimmed = trimmed || (info.EntriesAdded > info.Length && info.Length > 0)
	trimmed = trimmed || (info.FirstEntry.ID == "" && info.Length > 0)
	if !trimmed {
		return nil
	}
	groups, err := b.client.XInfoGroups(ctx, b.stream).Result()
	if err != nil {
		b.setDegraded()
		return err
	}
	for _, group := range groups {
		if group.Name != b.consumer.Group {
			continue
		}
		if info.FirstEntry.ID == "" && group.Lag > 0 && compareRedisStreamID(info.LastGeneratedID, group.LastDeliveredID) > 0 {
			b.setDegraded()
			return ErrStreamGap
		}
		gapID := info.MaxDeletedEntryID
		if gapID == "" || gapID == "0-0" {
			gapID = info.FirstEntry.ID
		}
		if compareRedisStreamID(gapID, group.LastDeliveredID) > 0 {
			b.setDegraded()
			return ErrStreamGap
		}
	}
	return nil
}

func (b *EventBus) Subscribe(ctx context.Context, handler func(sp.PlacementEvent) error) error {
	if err := b.EnsureConsumerGroup(ctx); err != nil {
		b.setDegraded()
		return err
	}
	if err := b.consume(ctx, "0", handler); err != nil {
		return err
	}
	return b.consume(ctx, ">", handler)
}

func (b *EventBus) consume(ctx context.Context, id string, handler func(sp.PlacementEvent) error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		streams, err := b.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    b.consumer.Group,
			Consumer: b.consumer.NodeSessionID,
			Streams:  []string{b.stream, id},
			Count:    10,
			Block:    50 * time.Millisecond,
		}).Result()
		if errors.Is(err, goredis.Nil) {
			if id == "0" {
				return nil
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			b.setDegraded()
			return err
		}
		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			if id == "0" {
				return nil
			}
			continue
		}
		for _, message := range streams[0].Messages {
			event, err := parseEvent(message.Values)
			if err != nil {
				b.setDegraded()
				return err
			}
			if err := handler(event); err != nil {
				b.setDegraded()
				return err
			}
			if err := b.client.XAck(ctx, b.stream, b.consumer.Group, message.ID).Err(); err != nil {
				b.setDegraded()
				return err
			}
		}
		if id == "0" {
			return nil
		}
	}
}

func (b *EventBus) setDegraded() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.degraded = true
}

func eventValues(event sp.PlacementEvent) map[string]any {
	return map[string]any{
		"type":              string(event.Type),
		"grain_key":         event.GrainKey.String(),
		"node_identity":     event.NodeIdentity,
		"node_type":         event.NodeType,
		"node_group":        event.NodeGroup,
		"node_name":         event.NodeName,
		"placement_version": event.PlacementVersion,
		"lease_version":     event.LeaseVersion,
	}
}

func parseEvent(values map[string]any) (sp.PlacementEvent, error) {
	eventType, ok := values["type"].(string)
	if !ok || eventType == "" {
		return sp.PlacementEvent{}, errors.New("redis stream event missing type")
	}
	event := sp.PlacementEvent{
		Type:         sp.EventType(eventType),
		GrainKey:     sp.GrainKey(stringValue(values["grain_key"])),
		NodeIdentity: stringValue(values["node_identity"]),
		NodeType:     stringValue(values["node_type"]),
		NodeGroup:    stringValue(values["node_group"]),
		NodeName:     stringValue(values["node_name"]),
		Time:         time.Now(),
	}
	event.PlacementVersion, _ = strconv.ParseInt(stringValue(values["placement_version"]), 10, 64)
	event.LeaseVersion, _ = strconv.ParseInt(stringValue(values["lease_version"]), 10, 64)
	return event, nil
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

func stringsHasBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func compareRedisStreamID(a string, b string) int {
	aTime, aSeq := splitRedisStreamID(a)
	bTime, bSeq := splitRedisStreamID(b)
	if aTime > bTime {
		return 1
	}
	if aTime < bTime {
		return -1
	}
	if aSeq > bSeq {
		return 1
	}
	if aSeq < bSeq {
		return -1
	}
	return 0
}

func splitRedisStreamID(id string) (int64, int64) {
	first, second, ok := strings.Cut(id, "-")
	if !ok {
		return 0, 0
	}
	firstInt, _ := strconv.ParseInt(first, 10, 64)
	secondInt, _ := strconv.ParseInt(second, 10, 64)
	return firstInt, secondInt
}
