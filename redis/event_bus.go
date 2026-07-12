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

const trimStreamLua = `
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end

local groups = redis.call("XINFO", "GROUPS", KEYS[1])
for _, group in ipairs(groups) do
	local pending = nil
	local lag = nil
	for index = 1, #group, 2 do
		if group[index] == "pending" then
			pending = group[index + 1]
		elseif group[index] == "lag" then
			lag = group[index + 1]
		end
	end
	if type(pending) ~= "number" or pending > 0 then
		return 0
	end
	if type(lag) ~= "number" or lag ~= 0 then
		return 0
	end
end

return redis.call("XTRIM", KEYS[1], "MAXLEN", "=", ARGV[1])
`

const replaceConsumerLua = `
local groups = redis.call("XINFO", "GROUPS", KEYS[1])
local old_found = false
local new_found = false

for _, group in ipairs(groups) do
	local name = nil
	local pending = nil
	local lag = nil
	for index = 1, #group, 2 do
		if group[index] == "name" then
			name = group[index + 1]
		elseif group[index] == "pending" then
			pending = group[index + 1]
		elseif group[index] == "lag" then
			lag = group[index + 1]
		end
	end
	if name == ARGV[1] then
		old_found = true
		if type(pending) ~= "number" or pending ~= 0 then
			return 1
		end
		if type(lag) ~= "number" or lag ~= 0 then
			return 1
		end
	elseif name == ARGV[2] then
		new_found = true
	end
end

if not old_found then
	if new_found then
		return 0
	end
	return redis.error_reply("NOGROUP old consumer group does not exist")
end

if not new_found then
	redis.call("XGROUP", "CREATE", KEYS[1], ARGV[2], "$")
end
redis.call("XGROUP", "DESTROY", KEYS[1], ARGV[1])
return 0
`

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
	if !validStreamConsumer(b.consumer) {
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
	if !validStreamConsumer(b.consumer) {
		b.setDegraded()
		return ErrSharedConsumerGroup
	}
	err := b.client.XGroupDestroy(ctx, b.stream, b.consumer.Group).Err()
	if err == goredis.Nil {
		return nil
	}
	return err
}

// CleanupConsumerGroup removes a previously valid node-session consumer group.
func (b *EventBus) CleanupConsumerGroup(ctx context.Context, consumer StreamConsumer) error {
	if !validStreamConsumer(consumer) {
		b.setDegraded()
		return ErrSharedConsumerGroup
	}
	err := b.client.XGroupDestroy(ctx, b.stream, consumer.Group).Err()
	if err == goredis.Nil {
		return nil
	}
	return err
}

// ReplaceConsumer removes an idle previous session group before creating this bus's group.
func (b *EventBus) ReplaceConsumer(ctx context.Context, old StreamConsumer) error {
	if !validStreamConsumer(old) || !validStreamConsumer(b.consumer) ||
		old.NodeIdentity != b.consumer.NodeIdentity ||
		old.NodeSessionID == b.consumer.NodeSessionID || old.Group == b.consumer.Group {
		b.setDegraded()
		return ErrSharedConsumerGroup
	}
	result, err := b.client.Eval(ctx, replaceConsumerLua, []string{b.stream}, old.Group, b.consumer.Group).Int64()
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			b.setDegraded()
		}
		return err
	}
	if result == 1 {
		b.setDegraded()
		return ErrPendingMessages
	}
	if result != 0 {
		b.setDegraded()
		return errors.New("redis consumer replacement returned an invalid result")
	}
	return nil
}

// Close removes this bus's session-specific consumer group.
func (b *EventBus) Close(ctx context.Context) error {
	err := b.DeleteConsumerGroup(ctx)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		b.setDegraded()
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
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			b.setDegraded()
		}
		return err
	}
	if len(pending) > 0 {
		b.setDegraded()
		return ErrPendingMessages
	}
	return nil
}

// Trim shortens the Stream only when every consumer group is caught up and has no pending messages.
func (b *EventBus) Trim(ctx context.Context, maxLen int64) error {
	return b.client.Eval(ctx, trimStreamLua, []string{b.stream}, maxLen).Err()
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
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		info, err := b.client.XInfoStream(ctx, b.stream).Result()
		if err != nil {
			if strings.Contains(err.Error(), "no such key") {
				return nil
			}
			if ctx.Err() == nil {
				b.setDegraded()
			}
			return err
		}
		gap, err := b.checkContinuitySnapshot(ctx, info)
		if err != nil {
			if ctx.Err() == nil {
				b.setDegraded()
			}
			return err
		}
		after, err := b.client.XInfoStream(ctx, b.stream).Result()
		if err != nil {
			if strings.Contains(err.Error(), "no such key") {
				return nil
			}
			if ctx.Err() == nil {
				b.setDegraded()
			}
			return err
		}
		if !sameContinuityStreamInfo(info, after) {
			continue
		}
		if gap {
			b.setDegraded()
			return ErrStreamGap
		}
		return nil
	}
	return ErrStreamContinuityUnstable
}

func (b *EventBus) checkContinuitySnapshot(ctx context.Context, info *goredis.XInfoStream) (bool, error) {
	groups, err := b.client.XInfoGroups(ctx, b.stream).Result()
	if err != nil {
		return false, err
	}
	var consumerGroup *goredis.XInfoGroup
	for i := range groups {
		if groups[i].Name == b.consumer.Group {
			consumerGroup = &groups[i]
			break
		}
	}
	if consumerGroup == nil {
		return false, nil
	}
	pending, err := b.client.XPending(ctx, b.stream, b.consumer.Group).Result()
	if err != nil {
		return false, err
	}
	firstEntryID := info.FirstEntry.ID
	if firstEntryID == "" && info.Length > 0 {
		messages, err := b.client.XRangeN(ctx, b.stream, "-", "+", 1).Result()
		if err != nil {
			return false, err
		}
		if len(messages) > 0 {
			firstEntryID = messages[0].ID
		}
	}
	maxDeleted := info.MaxDeletedEntryID != "" && info.MaxDeletedEntryID != "0-0"
	deletionObserved := maxDeleted || info.EntriesAdded > info.Length
	if pending.Count > 0 {
		pendingMissing := info.Length == 0
		pendingMissing = pendingMissing || (firstEntryID != "" && compareRedisStreamID(pending.Lower, firstEntryID) < 0)
		if pendingMissing {
			return true, nil
		}
		if deletionObserved {
			missing, err := b.pendingPayloadMissing(ctx, pending.Count)
			if err != nil {
				return false, err
			}
			if missing {
				return true, nil
			}
		}
	}
	if maxDeleted && compareRedisStreamID(info.MaxDeletedEntryID, consumerGroup.LastDeliveredID) > 0 {
		return true, nil
	}
	if info.EntriesAdded > 0 && consumerGroup.EntriesRead >= 0 && consumerGroup.Lag >= 0 {
		if consumerGroup.EntriesRead > info.EntriesAdded || consumerGroup.Lag > info.EntriesAdded-consumerGroup.EntriesRead {
			return true, nil
		}
		if consumerGroup.EntriesRead+consumerGroup.Lag < info.EntriesAdded {
			return true, nil
		}
		return false, nil
	}
	if info.EntriesAdded > info.Length {
		return true, nil
	}
	return false, nil
}

func (b *EventBus) pendingPayloadMissing(ctx context.Context, pendingCount int64) (bool, error) {
	const batchSize = int64(100)
	start := "-"
	var checked int64
	for checked < pendingCount {
		entries, err := b.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
			Stream: b.stream,
			Group:  b.consumer.Group,
			Start:  start,
			End:    "+",
			Count:  batchSize,
		}).Result()
		if err != nil {
			return false, err
		}
		if len(entries) == 0 {
			return false, nil
		}
		commands := make([]*goredis.XMessageSliceCmd, len(entries))
		_, err = b.client.Pipelined(ctx, func(pipe goredis.Pipeliner) error {
			for i, entry := range entries {
				commands[i] = pipe.XRangeN(ctx, b.stream, entry.ID, entry.ID, 1)
			}
			return nil
		})
		if err != nil {
			return false, err
		}
		for i, command := range commands {
			messages, err := command.Result()
			if err != nil {
				return false, err
			}
			entry := entries[i]
			if len(messages) != 1 || messages[0].ID != entry.ID {
				return true, nil
			}
		}
		checked += int64(len(entries))
		if int64(len(entries)) < batchSize {
			return false, nil
		}
		start = "(" + entries[len(entries)-1].ID
	}
	return false, nil
}

func sameContinuityStreamInfo(first, second *goredis.XInfoStream) bool {
	return first.Length == second.Length &&
		first.Groups == second.Groups &&
		first.LastGeneratedID == second.LastGeneratedID &&
		first.MaxDeletedEntryID == second.MaxDeletedEntryID &&
		first.EntriesAdded == second.EntriesAdded &&
		first.FirstEntry.ID == second.FirstEntry.ID &&
		first.LastEntry.ID == second.LastEntry.ID &&
		first.RecordedFirstEntryID == second.RecordedFirstEntryID
}

func (b *EventBus) Subscribe(ctx context.Context, handler func(sp.PlacementEvent) error) error {
	if err := b.EnsureConsumerGroup(ctx); err != nil {
		if ctx.Err() == nil {
			b.setDegraded()
		}
		return err
	}
	if err := b.CheckContinuity(ctx); err != nil {
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
				if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					return nil
				}
				b.setDegraded()
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			if err := b.client.XAck(ctx, b.stream, b.consumer.Group, message.ID).Err(); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				b.setDegraded()
				return err
			}
		}
	}
}

func (b *EventBus) setDegraded() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.degraded = true
}

func validStreamConsumer(consumer StreamConsumer) bool {
	return consumer.NodeIdentity != "" && consumer.NodeSessionID != "" &&
		consumer.Group == ConsumerGroupName(consumer.NodeIdentity, consumer.NodeSessionID)
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
