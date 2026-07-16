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
	logger   sp.Logger

	mu       sync.RWMutex
	degraded bool
}

// EventBusOption 配置 Redis EventBus。
type EventBusOption func(*EventBus)

// WithLogger 设置 Redis EventBus 使用的日志实例。
func WithLogger(logger sp.Logger) EventBusOption {
	return func(bus *EventBus) {
		if logger != nil {
			bus.logger = logger
		}
	}
}

const (
	redisScriptResultOK      int64 = 0
	redisScriptResultPending int64 = 1

	singleStreamEntryReadCount int64 = 1
	continuityMaxAttempts            = 3
	pendingPayloadBatchSize    int64 = 100
	eventReadBatchSize         int64 = 10
	eventReadBlockTimeout            = 50 * time.Millisecond

	redisStreamBeginningID  = "0"
	redisStreamNewEntriesID = ">"
	redisStreamNoDeletionID = "0-0"
)

func NewEventBus(client goredis.UniversalClient, consumer StreamConsumer, opts ...EventBusOption) *EventBus {
	bus := &EventBus{
		client:   client,
		stream:   EventsStreamKey(),
		consumer: consumer,
		logger:   sp.DefaultLogger(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(bus)
		}
	}
	return bus
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
		b.setDegraded("subscribe hint", err)
		return err
	}
	for {
		message, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			b.setDegraded("receive hint", err)
			return err
		}
		var event sp.PlacementEvent
		if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
			b.setDegraded("decode hint", err)
			return err
		}
		if err := handler(event); err != nil {
			b.setDegraded("handle hint", err)
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (b *EventBus) EnsureConsumerGroup(ctx context.Context) error {
	if !validStreamConsumer(b.consumer) {
		b.setDegraded("ensure consumer group", ErrSharedConsumerGroup)
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
		b.setDegraded("delete consumer group", ErrSharedConsumerGroup)
		return ErrSharedConsumerGroup
	}
	err := b.client.XGroupDestroy(ctx, b.stream, b.consumer.Group).Err()
	if err == goredis.Nil {
		return nil
	}
	return err
}

func (b *EventBus) CloseConsumerGroupIfIdle(ctx context.Context) error {
	if !validStreamConsumer(b.consumer) {
		b.setDegraded("close consumer group", ErrSharedConsumerGroup)
		return ErrSharedConsumerGroup
	}
	result, err := b.client.Eval(ctx, closeConsumerGroupIfIdleLua, []string{b.stream}, b.consumer.Group).Int64()
	if err != nil {
		return err
	}
	if result == redisScriptResultPending {
		return ErrPendingMessages
	}
	if result != redisScriptResultOK {
		return errors.New("redis consumer close returned an invalid result")
	}
	return nil
}

// CleanupConsumerGroup removes a previously valid node-session consumer group.
func (b *EventBus) CleanupConsumerGroup(ctx context.Context, consumer StreamConsumer) error {
	if !validStreamConsumer(consumer) {
		b.setDegraded("cleanup consumer group", ErrSharedConsumerGroup)
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
		b.setDegraded("replace consumer", ErrSharedConsumerGroup)
		return ErrSharedConsumerGroup
	}
	result, err := b.client.Eval(ctx, replaceConsumerLua, []string{b.stream}, old.Group, b.consumer.Group).Int64()
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			b.setDegraded("replace consumer", err)
		}
		return err
	}
	if result == redisScriptResultPending {
		b.setDegraded("replace consumer", ErrPendingMessages)
		return ErrPendingMessages
	}
	if result != redisScriptResultOK {
		err := errors.New("redis consumer replacement returned an invalid result")
		b.setDegraded("replace consumer", err)
		return err
	}
	return nil
}

// Close removes this bus's session-specific consumer group only when it is idle.
func (b *EventBus) Close(ctx context.Context) error {
	err := b.CloseConsumerGroupIfIdle(ctx)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		b.setDegraded("close event bus", err)
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
		Count:  singleStreamEntryReadCount,
	}).Result()
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			b.setDegraded("check pending", err)
		}
		return err
	}
	if len(pending) > 0 {
		b.setDegraded("check pending", ErrPendingMessages)
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
				b.setDegraded("trim stream", err)
				return err
			}
		}
	}
}

// CheckContinuity enters degraded mode when Redis reports that this group may have missed trimmed events.
func (b *EventBus) CheckContinuity(ctx context.Context) error {
	for attempt := 0; attempt < continuityMaxAttempts; attempt++ {
		info, err := b.client.XInfoStream(ctx, b.stream).Result()
		if err != nil {
			if strings.Contains(err.Error(), "no such key") {
				return nil
			}
			if ctx.Err() == nil {
				b.setDegraded("check continuity stream", err)
			}
			return err
		}
		gap, err := b.checkContinuitySnapshot(ctx, info)
		if err != nil {
			if ctx.Err() == nil {
				b.setDegraded("check continuity snapshot", err)
			}
			return err
		}
		after, err := b.client.XInfoStream(ctx, b.stream).Result()
		if err != nil {
			if strings.Contains(err.Error(), "no such key") {
				return nil
			}
			if ctx.Err() == nil {
				b.setDegraded("confirm continuity stream", err)
			}
			return err
		}
		if !sameContinuityStreamInfo(info, after) {
			continue
		}
		if gap {
			b.setDegraded("check continuity", ErrStreamGap)
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
		messages, err := b.client.XRangeN(ctx, b.stream, "-", "+", singleStreamEntryReadCount).Result()
		if err != nil {
			return false, err
		}
		if len(messages) > 0 {
			firstEntryID = messages[0].ID
		}
	}
	maxDeleted := info.MaxDeletedEntryID != "" && info.MaxDeletedEntryID != redisStreamNoDeletionID
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
	if consumerGroup.Pending == 0 && consumerGroup.Lag == 0 &&
		consumerGroup.LastDeliveredID != "" && consumerGroup.LastDeliveredID == info.LastGeneratedID {
		return false, nil
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
	start := "-"
	var checked int64
	for checked < pendingCount {
		entries, err := b.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
			Stream: b.stream,
			Group:  b.consumer.Group,
			Start:  start,
			End:    "+",
			Count:  pendingPayloadBatchSize,
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
				commands[i] = pipe.XRangeN(ctx, b.stream, entry.ID, entry.ID, singleStreamEntryReadCount)
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
		if int64(len(entries)) < pendingPayloadBatchSize {
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
			b.setDegraded("subscribe ensure consumer group", err)
		}
		return err
	}
	if err := b.CheckContinuity(ctx); err != nil {
		return err
	}
	if err := b.consume(ctx, redisStreamBeginningID, handler); err != nil {
		return err
	}
	return b.consume(ctx, redisStreamNewEntriesID, handler)
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
			Count:    eventReadBatchSize,
			Block:    eventReadBlockTimeout,
		}).Result()
		if errors.Is(err, goredis.Nil) {
			if id == redisStreamBeginningID {
				return nil
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			b.setDegraded("consume stream", err)
			return err
		}
		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			if id == redisStreamBeginningID {
				return nil
			}
			continue
		}
		for _, message := range streams[0].Messages {
			event, err := parseEvent(message.Values)
			if err != nil {
				b.setDegraded("decode stream event", err)
				return err
			}
			if err := handler(event); err != nil {
				if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					return nil
				}
				b.setDegraded("handle stream event", err)
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			if err := b.client.XAck(ctx, b.stream, b.consumer.Group, message.ID).Err(); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				b.setDegraded("ack stream event", err)
				return err
			}
		}
	}
}

func (b *EventBus) setDegraded(operation string, err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	b.mu.Lock()
	if b.degraded {
		b.mu.Unlock()
		return
	}
	b.degraded = true
	b.mu.Unlock()
	b.logger.Errorf(
		"stable-placement/redis: event bus degraded operation=%q consumer_group=%q node_identity=%q node_session_id=%q: %v",
		operation,
		b.consumer.Group,
		b.consumer.NodeIdentity,
		b.consumer.NodeSessionID,
		err,
	)
}

func validStreamConsumer(consumer StreamConsumer) bool {
	return consumer.NodeIdentity != "" && consumer.NodeSessionID != "" &&
		consumer.Group == ConsumerGroupName(consumer.NodeIdentity, consumer.NodeSessionID)
}

func eventValues(event sp.PlacementEvent) map[string]any {
	return map[string]any{
		"type":               string(event.Type),
		"grain_key":          event.GrainKey.String(),
		"placement_id":       event.PlacementID,
		"node_identity":      event.NodeIdentity,
		"node_session_id":    event.NodeSessionID,
		"node_type":          event.NodeType,
		"node_group":         event.NodeGroup,
		"node_name":          event.NodeName,
		"placement_version":  event.PlacementVersion,
		"node_lease_version": event.NodeLeaseVersion,
	}
}

func parseEvent(values map[string]any) (sp.PlacementEvent, error) {
	eventType, ok := values["type"].(string)
	if !ok || eventType == "" {
		return sp.PlacementEvent{}, errors.New("redis stream event missing type")
	}
	event := sp.PlacementEvent{
		Type:          sp.EventType(eventType),
		GrainKey:      sp.GrainKey(stringValue(values["grain_key"])),
		PlacementID:   stringValue(values["placement_id"]),
		NodeIdentity:  stringValue(values["node_identity"]),
		NodeSessionID: stringValue(values["node_session_id"]),
		NodeType:      stringValue(values["node_type"]),
		NodeGroup:     stringValue(values["node_group"]),
		NodeName:      stringValue(values["node_name"]),
		Time:          time.Now(),
	}
	event.PlacementVersion, _ = strconv.ParseInt(stringValue(values["placement_version"]), 10, 64)
	event.NodeLeaseVersion, _ = strconv.ParseInt(stringValue(values["node_lease_version"]), 10, 64)
	if event.Type == sp.EventNodeLeaseExpired && (event.NodeIdentity == "" || event.NodeSessionID == "" || event.NodeLeaseVersion <= 0) {
		return sp.PlacementEvent{}, errors.New("redis stream node lease event missing identity, session, or version")
	}
	return event, nil
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
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
