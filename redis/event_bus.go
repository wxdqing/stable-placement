package redis

import (
	"context"
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
