package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	sp "github.com/wxdqing/stable-placement"
)

type trimRaceClient struct {
	goredis.UniversalClient
	once        sync.Once
	mutate      func(context.Context) error
	mutationErr error
}

type xAckErrorClient struct {
	goredis.UniversalClient
	err error
}

type consumerLifecycleErrorClient struct {
	goredis.UniversalClient
	pendingErr    error
	pendingExtErr error
	destroyErr    error
	ensureErr     error
}

type consumerReplaceRaceClient struct {
	goredis.UniversalClient
	once   sync.Once
	mutate func(context.Context) error
	err    error
}

func (c *consumerReplaceRaceClient) trigger(ctx context.Context) error {
	c.once.Do(func() { c.err = c.mutate(ctx) })
	return c.err
}

func (c *consumerReplaceRaceClient) XPending(ctx context.Context, stream, group string) *goredis.XPendingCmd {
	pending, err := c.UniversalClient.XPending(ctx, stream, group).Result()
	if err == nil {
		err = c.trigger(ctx)
	}
	cmd := goredis.NewXPendingCmd(ctx, "xpending", stream, group)
	cmd.SetVal(pending)
	cmd.SetErr(err)
	return cmd
}

func (c *consumerReplaceRaceClient) Eval(ctx context.Context, script string, keys []string, args ...any) *goredis.Cmd {
	if err := c.trigger(ctx); err != nil {
		return goredis.NewCmdResult(nil, err)
	}
	return c.UniversalClient.Eval(ctx, script, keys, args...)
}

func (c consumerLifecycleErrorClient) XPending(ctx context.Context, stream, group string) *goredis.XPendingCmd {
	if c.pendingErr == nil {
		return c.UniversalClient.XPending(ctx, stream, group)
	}
	cmd := goredis.NewXPendingCmd(ctx, "xpending", stream, group)
	cmd.SetErr(c.pendingErr)
	return cmd
}

func (c consumerLifecycleErrorClient) XPendingExt(ctx context.Context, args *goredis.XPendingExtArgs) *goredis.XPendingExtCmd {
	if c.pendingExtErr == nil {
		return c.UniversalClient.XPendingExt(ctx, args)
	}
	cmd := goredis.NewXPendingExtCmd(ctx, "xpending", args.Stream, args.Group)
	cmd.SetErr(c.pendingExtErr)
	return cmd
}

func (c consumerLifecycleErrorClient) XGroupDestroy(ctx context.Context, stream, group string) *goredis.IntCmd {
	if c.destroyErr == nil {
		return c.UniversalClient.XGroupDestroy(ctx, stream, group)
	}
	cmd := goredis.NewIntCmd(ctx, "xgroup", "destroy", stream, group)
	cmd.SetErr(c.destroyErr)
	return cmd
}

func (c consumerLifecycleErrorClient) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *goredis.StatusCmd {
	if c.ensureErr == nil {
		return c.UniversalClient.XGroupCreateMkStream(ctx, stream, group, start)
	}
	cmd := goredis.NewStatusCmd(ctx, "xgroup", "create", stream, group, start, "mkstream")
	cmd.SetErr(c.ensureErr)
	return cmd
}

func (c consumerLifecycleErrorClient) Eval(ctx context.Context, script string, keys []string, args ...any) *goredis.Cmd {
	for _, err := range []error{c.pendingErr, c.destroyErr, c.ensureErr} {
		if err != nil {
			return goredis.NewCmdResult(nil, err)
		}
	}
	return c.UniversalClient.Eval(ctx, script, keys, args...)
}

type limitedPendingReadClient struct {
	goredis.UniversalClient
}

type continuityMetadataClient struct {
	goredis.UniversalClient
	entriesAdded int64
	entriesRead  int64
	lag          int64
	maxDeletedID string
	pending      *goredis.XPending
	pendingExt   []goredis.XPendingExt
}

type continuityRaceClient struct {
	goredis.UniversalClient
	mu                 sync.Mutex
	mutateEvery        bool
	entriesAddedOffset int64
	mutate             func(context.Context) error
	infoCalls          int
	err                error
}

func (c *continuityRaceClient) XInfoStream(ctx context.Context, stream string) *goredis.XInfoStreamCmd {
	info, err := c.UniversalClient.XInfoStream(ctx, stream).Result()
	if err == nil {
		info.EntriesAdded = info.Length + c.entriesAddedOffset
		c.mu.Lock()
		c.infoCalls++
		if c.err == nil && (c.mutateEvery || c.infoCalls == 1) {
			c.err = c.mutate(ctx)
		}
		err = c.err
		c.mu.Unlock()
	}
	cmd := goredis.NewXInfoStreamCmd(ctx, stream)
	cmd.SetVal(info)
	cmd.SetErr(err)
	return cmd
}

type pendingPayloadPipelineClient struct {
	goredis.UniversalClient
	entries        []goredis.XPendingExt
	pendingCounts  []int
	pipelineCounts []int
}

func (c *pendingPayloadPipelineClient) XPendingExt(ctx context.Context, args *goredis.XPendingExtArgs) *goredis.XPendingExtCmd {
	start := 0
	if len(args.Start) > 1 && args.Start[0] == '(' {
		for start < len(c.entries) && compareRedisStreamID(c.entries[start].ID, args.Start[1:]) <= 0 {
			start++
		}
	}
	end := start + int(args.Count)
	if end > len(c.entries) {
		end = len(c.entries)
	}
	page := append([]goredis.XPendingExt(nil), c.entries[start:end]...)
	c.pendingCounts = append(c.pendingCounts, len(page))
	cmd := goredis.NewXPendingExtCmd(ctx, "xpending", args.Stream, args.Group)
	cmd.SetVal(page)
	return cmd
}

func (c *pendingPayloadPipelineClient) Pipelined(ctx context.Context, fn func(goredis.Pipeliner) error) ([]goredis.Cmder, error) {
	cmds, err := c.UniversalClient.Pipelined(ctx, fn)
	c.pipelineCounts = append(c.pipelineCounts, len(cmds))
	return cmds, err
}

func (c *continuityRaceClient) XInfoGroups(ctx context.Context, stream string) *goredis.XInfoGroupsCmd {
	groups, err := c.UniversalClient.XInfoGroups(ctx, stream).Result()
	if err == nil {
		for i := range groups {
			groups[i].EntriesRead = 0
		}
	}
	cmd := goredis.NewXInfoGroupsCmd(ctx, stream)
	cmd.SetVal(groups)
	cmd.SetErr(err)
	return cmd
}

func (c continuityMetadataClient) XInfoStream(ctx context.Context, stream string) *goredis.XInfoStreamCmd {
	info, err := c.UniversalClient.XInfoStream(ctx, stream).Result()
	if err == nil {
		info.EntriesAdded = c.entriesAdded
		info.MaxDeletedEntryID = c.maxDeletedID
	}
	cmd := goredis.NewXInfoStreamCmd(ctx, stream)
	cmd.SetVal(info)
	cmd.SetErr(err)
	return cmd
}

func (c continuityMetadataClient) XInfoGroups(ctx context.Context, stream string) *goredis.XInfoGroupsCmd {
	groups, err := c.UniversalClient.XInfoGroups(ctx, stream).Result()
	if err == nil {
		for i := range groups {
			groups[i].EntriesRead = c.entriesRead
			groups[i].Lag = c.lag
		}
	}
	cmd := goredis.NewXInfoGroupsCmd(ctx, stream)
	cmd.SetVal(groups)
	cmd.SetErr(err)
	return cmd
}

func (c continuityMetadataClient) XPending(ctx context.Context, stream, group string) *goredis.XPendingCmd {
	if c.pending == nil {
		return c.UniversalClient.XPending(ctx, stream, group)
	}
	cmd := goredis.NewXPendingCmd(ctx, "xpending", stream, group)
	cmd.SetVal(c.pending)
	return cmd
}

func (c continuityMetadataClient) XPendingExt(ctx context.Context, args *goredis.XPendingExtArgs) *goredis.XPendingExtCmd {
	if c.pendingExt == nil {
		return c.UniversalClient.XPendingExt(ctx, args)
	}
	cmd := goredis.NewXPendingExtCmd(ctx, "xpending", args.Stream, args.Group)
	cmd.SetVal(c.pendingExt)
	return cmd
}

func (c limitedPendingReadClient) XReadGroup(ctx context.Context, args *goredis.XReadGroupArgs) *goredis.XStreamSliceCmd {
	streams, err := c.UniversalClient.XReadGroup(ctx, args).Result()
	if err == nil && len(args.Streams) == 2 && args.Streams[1] == "0" && len(streams) > 0 && int64(len(streams[0].Messages)) > args.Count {
		streams[0].Messages = streams[0].Messages[:args.Count]
	}
	return goredis.NewXStreamSliceCmdResult(streams, err)
}

func (c xAckErrorClient) XAck(context.Context, string, string, ...string) *goredis.IntCmd {
	return goredis.NewIntResult(0, c.err)
}

func (c *trimRaceClient) trigger(ctx context.Context) error {
	c.once.Do(func() {
		c.mutationErr = c.mutate(ctx)
	})
	return c.mutationErr
}

func (c *trimRaceClient) XInfoGroups(ctx context.Context, stream string) *goredis.XInfoGroupsCmd {
	groups, err := c.UniversalClient.XInfoGroups(ctx, stream).Result()
	if err == nil {
		err = c.trigger(ctx)
	}
	cmd := goredis.NewXInfoGroupsCmd(ctx, stream)
	cmd.SetVal(groups)
	cmd.SetErr(err)
	return cmd
}

func (c *trimRaceClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *goredis.Cmd {
	if err := c.trigger(ctx); err != nil {
		return goredis.NewCmdResult(nil, err)
	}
	return c.UniversalClient.Eval(ctx, script, keys, args...)
}

func TestRedisEventBusBroadcastsToUniqueConsumerGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})

	firstConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("first consumer error: %v", err)
	}
	secondConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-2", NodeSessionID: "session-b"})
	if err != nil {
		t.Fatalf("second consumer error: %v", err)
	}
	first := NewEventBus(client, firstConsumer)
	second := NewEventBus(client, secondConsumer)
	if err := first.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("first EnsureConsumerGroup error: %v", err)
	}
	if err := second.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("second EnsureConsumerGroup error: %v", err)
	}
	event := sp.PlacementEvent{
		Type:             sp.EventPlacementTransferred,
		GrainKey:         sp.GrainKey("Player/10001"),
		NodeIdentity:     "game/default/game-2",
		PlacementVersion: 2,
		NodeSessionID:    "session-a",
		NodeLeaseVersion: 1,
	}
	if err := first.Publish(ctx, event); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	gotFirst := make(chan sp.PlacementEvent, 1)
	gotSecond := make(chan sp.PlacementEvent, 1)
	firstCtx, firstCancel := context.WithCancel(ctx)
	secondCtx, secondCancel := context.WithCancel(ctx)
	defer firstCancel()
	defer secondCancel()
	go func() {
		_ = first.Subscribe(firstCtx, func(event sp.PlacementEvent) error {
			gotFirst <- event
			firstCancel()
			return nil
		})
	}()
	go func() {
		_ = second.Subscribe(secondCtx, func(event sp.PlacementEvent) error {
			gotSecond <- event
			secondCancel()
			return nil
		})
	}()

	for name, ch := range map[string]chan sp.PlacementEvent{"first": gotFirst, "second": gotSecond} {
		select {
		case got := <-ch:
			if got.Type != event.Type || got.GrainKey != event.GrainKey {
				t.Fatalf("%s event = %+v", name, got)
			}
		case <-ctx.Done():
			t.Fatalf("%s subscriber did not receive event", name)
		}
	}
}

func TestRedisEventBusHandlerErrorEntersDegraded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	handlerErr := errors.New("handler failed")
	err = bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		return handlerErr
	})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("Subscribe err = %v", err)
	}
	if !bus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}

func TestRedisEventBusRetriesPendingMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementReleased, GrainKey: sp.GrainKey("Player/10001")}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	handlerErr := errors.New("first handler failed")
	if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		return handlerErr
	}); !errors.Is(err, handlerErr) {
		t.Fatalf("first Subscribe err = %v", err)
	}

	retryBus := NewEventBus(client, consumer)
	got := make(chan sp.PlacementEvent, 1)
	retryCtx, retryCancel := context.WithCancel(ctx)
	defer retryCancel()
	go func() {
		_ = retryBus.Subscribe(retryCtx, func(event sp.PlacementEvent) error {
			got <- event
			retryCancel()
			return nil
		})
	}()
	select {
	case event := <-got:
		if event.Type != sp.EventPlacementReleased {
			t.Fatalf("retried event = %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("pending event was not retried")
	}
}

func TestRedisEventBusDrainsAllPendingBeforeNewMessages(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	for i := 0; i < 12; i++ {
		event := sp.PlacementEvent{Type: sp.EventPlacementReleased, GrainKey: sp.GrainKey("Player/" + strconv.Itoa(i))}
		if err := bus.Publish(ctx, event); err != nil {
			t.Fatalf("Publish pending %d error: %v", i, err)
		}
	}
	if err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: consumer.Group, Consumer: consumer.NodeSessionID,
		Streams: []string{bus.StreamKey(), ">"}, Count: 12,
	}).Err(); err != nil {
		t.Fatalf("create pending error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{
		Type: sp.EventPlacementTransferred, GrainKey: sp.GrainKey("Player/new"),
	}); err != nil {
		t.Fatalf("Publish new error: %v", err)
	}
	bus.client = limitedPendingReadClient{UniversalClient: client}

	stop := errors.New("new message reached")
	var got []sp.GrainKey
	err = bus.Subscribe(ctx, func(event sp.PlacementEvent) error {
		got = append(got, event.GrainKey)
		if event.GrainKey == sp.GrainKey("Player/new") {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("Subscribe err = %v, want stop", err)
	}
	if len(got) != 13 {
		t.Fatalf("handled messages = %d, want 13: %v", len(got), got)
	}
	for i := 0; i < 12; i++ {
		want := sp.GrainKey("Player/" + strconv.Itoa(i))
		if got[i] != want {
			t.Fatalf("handled message %d = %q, want pending %q", i, got[i], want)
		}
	}
}

func TestRedisEventBusHandlerCancellationKeepsMessagePendingWithoutDegrading(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		cancel()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	if bus.IsDegraded() {
		t.Fatal("handler cancellation put bus in degraded mode")
	}
	pending, err := client.XPending(context.Background(), bus.StreamKey(), consumer.Group).Result()
	if err != nil {
		t.Fatalf("XPending error: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("pending count = %d, want 1", pending.Count)
	}
}

func TestRedisEventBusHandlerCancellationErrorKeepsMessagePendingWithoutDegrading(t *testing.T) {
	for _, handlerErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(handlerErr.Error(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := miniredis.RunT(t)
			client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
			consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
			if err != nil {
				t.Fatalf("consumer error: %v", err)
			}
			bus := NewEventBus(client, consumer)
			if err := bus.EnsureConsumerGroup(ctx); err != nil {
				t.Fatalf("EnsureConsumerGroup error: %v", err)
			}
			if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
				t.Fatalf("Publish error: %v", err)
			}

			if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error {
				cancel()
				return handlerErr
			}); err != nil {
				t.Fatalf("Subscribe error: %v", err)
			}
			if bus.IsDegraded() {
				t.Fatal("handler cancellation error put bus in degraded mode")
			}
			pending, err := client.XPending(context.Background(), bus.StreamKey(), consumer.Group).Result()
			if err != nil {
				t.Fatalf("XPending error: %v", err)
			}
			if pending.Count != 1 {
				t.Fatalf("pending count = %d, want 1", pending.Count)
			}
		})
	}
}

func TestRedisEventBusAckCancellationDoesNotDegrade(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(base, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	bus.client = xAckErrorClient{UniversalClient: base, err: context.DeadlineExceeded}

	if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error { return nil }); err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	if bus.IsDegraded() {
		t.Fatal("ack deadline put bus in degraded mode")
	}
}

func TestRedisEventBusMalformedEventEntersDegraded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: EventsStreamKey(),
		Values: map[string]any{"grain_key": "Player/10001"},
	}).Err(); err != nil {
		t.Fatalf("XAdd malformed error: %v", err)
	}

	err = bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		t.Fatal("handler should not be called for malformed event")
		return nil
	})
	if err == nil {
		t.Fatal("Subscribe succeeded for malformed event")
	}
	if !bus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}

func TestRedisEventBusRejectsMismatchedConsumerGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer := StreamConsumer{
		NodeIdentity:  "game/default/game-2",
		NodeSessionID: "session-b",
		Group:         ConsumerGroupName("game/default/game-1", "session-a"),
	}
	bus := NewEventBus(client, consumer)

	err := bus.EnsureConsumerGroup(ctx)
	if !errors.Is(err, ErrSharedConsumerGroup) {
		t.Fatalf("EnsureConsumerGroup err = %v, want ErrSharedConsumerGroup", err)
	}
	if !bus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}

func TestRedisEventBusSharedConsumerGroupWouldLoadBalanceAndIsRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	sharedGroup := ConsumerGroupName("game/default/game-1", "session-a")
	if err := client.XGroupCreateMkStream(ctx, EventsStreamKey(), sharedGroup, "$").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream error: %v", err)
	}
	bus := NewEventBus(client, StreamConsumer{
		NodeIdentity:  "game/default/game-1",
		NodeSessionID: "session-a",
		Group:         sharedGroup,
	})
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	first, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    sharedGroup,
		Consumer: "session-a",
		Streams:  []string{EventsStreamKey(), ">"},
		Count:    1,
		Block:    10 * time.Millisecond,
	}).Result()
	if err != nil {
		t.Fatalf("first XReadGroup error: %v", err)
	}
	if len(first) != 1 || len(first[0].Messages) != 1 {
		t.Fatalf("first consumer messages = %+v", first)
	}
	second, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    sharedGroup,
		Consumer: "session-b",
		Streams:  []string{EventsStreamKey(), ">"},
		Count:    1,
		Block:    10 * time.Millisecond,
	}).Result()
	if err != goredis.Nil {
		t.Fatalf("second XReadGroup err = %v, messages = %+v", err, second)
	}

	rejected := NewEventBus(client, StreamConsumer{
		NodeIdentity:  "game/default/game-2",
		NodeSessionID: "session-b",
		Group:         sharedGroup,
	})
	if err := rejected.EnsureConsumerGroup(ctx); !errors.Is(err, ErrSharedConsumerGroup) {
		t.Fatalf("EnsureConsumerGroup err = %v, want ErrSharedConsumerGroup", err)
	}
}

func TestRedisEventBusCleansOldSessionGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("old consumer error: %v", err)
	}
	newConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}
	oldBus := NewEventBus(client, oldConsumer)
	if err := oldBus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("old EnsureConsumerGroup error: %v", err)
	}
	newBus := NewEventBus(client, newConsumer)
	if err := newBus.CleanupConsumerGroup(ctx, oldConsumer); err != nil {
		t.Fatalf("CleanupConsumerGroup error: %v", err)
	}
	if err := newBus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("new EnsureConsumerGroup error: %v", err)
	}
	groups, err := client.XInfoGroups(ctx, EventsStreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups error: %v", err)
	}
	for _, group := range groups {
		if group.Name == oldConsumer.Group {
			t.Fatalf("old consumer group still exists: %+v", groups)
		}
	}
}

func TestRedisEventBusReplaceConsumerAndCloseConsumerGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("old consumer error: %v", err)
	}
	newConsumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}
	if err := NewEventBus(client, oldConsumer).EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("old EnsureConsumerGroup error: %v", err)
	}
	bus := NewEventBus(client, newConsumer)
	if err := bus.ReplaceConsumer(ctx, oldConsumer); err != nil {
		t.Fatalf("ReplaceConsumer error: %v", err)
	}
	groups, err := client.XInfoGroups(ctx, bus.StreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups after replace error: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != newConsumer.Group {
		t.Fatalf("groups after replace = %+v, want only %q", groups, newConsumer.Group)
	}
	if err := bus.Close(ctx); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	groups, err = client.XInfoGroups(ctx, bus.StreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups after close error: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("groups after close = %+v, want none", groups)
	}
}

func TestRedisEventBusReplaceConsumerRejectsInvalidOldGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	err = bus.ReplaceConsumer(ctx, StreamConsumer{
		NodeIdentity: "game/default/game-1", NodeSessionID: "session-a", Group: "shared",
	})
	if !errors.Is(err, ErrSharedConsumerGroup) {
		t.Fatalf("ReplaceConsumer error = %v, want ErrSharedConsumerGroup", err)
	}
	if !bus.IsDegraded() {
		t.Fatal("invalid old group did not degrade bus")
	}
	if server.Exists(bus.StreamKey()) {
		t.Fatal("invalid old group created the current group")
	}
}

func TestRedisEventBusReplaceConsumerRejectsMissingOldGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	newConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	bus := NewEventBus(client, newConsumer)
	if err := bus.ReplaceConsumer(ctx, oldConsumer); err == nil {
		t.Fatal("ReplaceConsumer succeeded with missing old group")
	}
	if !bus.IsDegraded() {
		t.Fatal("missing old group did not degrade bus")
	}
	if server.Exists(bus.StreamKey()) {
		t.Fatal("missing old group created the current group")
	}
}

func TestRedisEventBusReplaceConsumerKeepsOldPendingGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	newConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	oldBus := NewEventBus(client, oldConsumer)
	if err := oldBus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("old EnsureConsumerGroup error: %v", err)
	}
	if err := oldBus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	if err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: oldConsumer.Group, Consumer: oldConsumer.NodeSessionID,
		Streams: []string{oldBus.StreamKey(), ">"}, Count: 1,
	}).Err(); err != nil {
		t.Fatalf("XReadGroup error: %v", err)
	}
	bus := NewEventBus(client, newConsumer)
	if err := bus.ReplaceConsumer(ctx, oldConsumer); !errors.Is(err, ErrPendingMessages) {
		t.Fatalf("ReplaceConsumer error = %v, want ErrPendingMessages", err)
	}
	if !bus.IsDegraded() {
		t.Fatal("pending old group did not degrade bus")
	}
	groups, err := client.XInfoGroups(ctx, bus.StreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups error: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != oldConsumer.Group {
		t.Fatalf("groups after rejected replace = %+v, want only old group", groups)
	}
}

func TestRedisEventBusReplaceConsumerAtomicallyKeepsNewPendingMessage(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	newConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	oldBus := NewEventBus(base, oldConsumer)
	if err := oldBus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("old EnsureConsumerGroup error: %v", err)
	}
	if err := oldBus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	hooked := &consumerReplaceRaceClient{UniversalClient: base}
	hooked.mutate = func(ctx context.Context) error {
		return base.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group: oldConsumer.Group, Consumer: oldConsumer.NodeSessionID,
			Streams: []string{oldBus.StreamKey(), ">"}, Count: 1,
		}).Err()
	}
	bus := NewEventBus(hooked, newConsumer)
	if err := bus.ReplaceConsumer(ctx, oldConsumer); !errors.Is(err, ErrPendingMessages) {
		t.Fatalf("ReplaceConsumer error = %v, want ErrPendingMessages", err)
	}
	groups, err := base.XInfoGroups(ctx, bus.StreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups error: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != oldConsumer.Group || groups[0].Pending != 1 {
		t.Fatalf("groups after pending race = %+v, want pending old group only", groups)
	}
}

func TestRedisEventBusReplaceConsumerKeepsLaggingOldGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	newConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	oldBus := NewEventBus(client, oldConsumer)
	if err := oldBus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("old EnsureConsumerGroup error: %v", err)
	}
	if err := oldBus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	bus := NewEventBus(client, newConsumer)
	if err := bus.ReplaceConsumer(ctx, oldConsumer); !errors.Is(err, ErrPendingMessages) {
		t.Fatalf("ReplaceConsumer error = %v, want ErrPendingMessages", err)
	}
	groups, err := client.XInfoGroups(ctx, bus.StreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups error: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != oldConsumer.Group {
		t.Fatalf("groups after lag rejection = %+v, want old group only", groups)
	}
}

func TestRedisEventBusReplaceConsumerRejectsUnrelatedConsumers(t *testing.T) {
	cases := []struct {
		name string
		old  sp.Node
		new  sp.Node
	}{
		{
			name: "same session",
			old:  sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"},
			new:  sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"},
		},
		{
			name: "different identity",
			old:  sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"},
			new:  sp.Node{NodeIdentity: "game/default/game-2", NodeSessionID: "session-b"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			server := miniredis.RunT(t)
			client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
			oldConsumer, _ := NewStreamConsumer(tc.old)
			newConsumer, _ := NewStreamConsumer(tc.new)
			oldBus := NewEventBus(client, oldConsumer)
			if err := oldBus.EnsureConsumerGroup(ctx); err != nil {
				t.Fatalf("old EnsureConsumerGroup error: %v", err)
			}
			if err := oldBus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
				t.Fatalf("Publish error: %v", err)
			}
			bus := NewEventBus(client, newConsumer)
			if err := bus.ReplaceConsumer(ctx, oldConsumer); !errors.Is(err, ErrSharedConsumerGroup) {
				t.Fatalf("ReplaceConsumer error = %v, want ErrSharedConsumerGroup", err)
			}
			groups, err := client.XInfoGroups(ctx, bus.StreamKey()).Result()
			if err != nil {
				t.Fatalf("XInfoGroups error: %v", err)
			}
			if len(groups) != 1 || groups[0].Name != oldConsumer.Group {
				t.Fatalf("groups after rejected replace = %+v, want old group only", groups)
			}
		})
	}
}

func TestRedisEventBusReplaceConsumerValidatesCurrentConsumer(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err := NewEventBus(client, oldConsumer).EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("old EnsureConsumerGroup error: %v", err)
	}
	bus := NewEventBus(client, StreamConsumer{
		NodeIdentity: "game/default/game-1", NodeSessionID: "session-b", Group: "arbitrary-group",
	})
	if err := bus.ReplaceConsumer(ctx, oldConsumer); !errors.Is(err, ErrSharedConsumerGroup) {
		t.Fatalf("ReplaceConsumer error = %v, want ErrSharedConsumerGroup", err)
	}
	groups, err := client.XInfoGroups(ctx, bus.StreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups error: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != oldConsumer.Group {
		t.Fatalf("groups after invalid current consumer = %+v, want old group only", groups)
	}
}

func TestRedisEventBusReplaceConsumerCreateFailureKeepsOldGroupAndRetries(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	newConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	if err := NewEventBus(base, oldConsumer).EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("old EnsureConsumerGroup error: %v", err)
	}
	redisErr := errors.New("create group failure")
	bus := NewEventBus(consumerLifecycleErrorClient{UniversalClient: base, ensureErr: redisErr}, newConsumer)
	if err := bus.ReplaceConsumer(ctx, oldConsumer); !errors.Is(err, redisErr) {
		t.Fatalf("ReplaceConsumer error = %v, want create error", err)
	}
	groups, err := base.XInfoGroups(ctx, bus.StreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups after failure error: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != oldConsumer.Group {
		t.Fatalf("groups after create failure = %+v, want old group only", groups)
	}
	bus = NewEventBus(base, newConsumer)
	if err := bus.ReplaceConsumer(ctx, oldConsumer); err != nil {
		t.Fatalf("ReplaceConsumer retry error: %v", err)
	}
}

func TestRedisEventBusReplaceConsumerRetryAfterCompletedMigration(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	oldConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	newConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
	if err := NewEventBus(client, oldConsumer).EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("old EnsureConsumerGroup error: %v", err)
	}
	bus := NewEventBus(client, newConsumer)
	if err := bus.ReplaceConsumer(ctx, oldConsumer); err != nil {
		t.Fatalf("first ReplaceConsumer error: %v", err)
	}
	if err := bus.ReplaceConsumer(ctx, oldConsumer); err != nil {
		t.Fatalf("retry ReplaceConsumer error: %v", err)
	}
}

func TestRedisEventBusReplaceConsumerRedisErrorsDegrade(t *testing.T) {
	stages := []struct {
		name string
		wrap func(goredis.UniversalClient, error) goredis.UniversalClient
	}{
		{name: "pending", wrap: func(client goredis.UniversalClient, err error) goredis.UniversalClient {
			return consumerLifecycleErrorClient{UniversalClient: client, pendingErr: err}
		}},
		{name: "cleanup", wrap: func(client goredis.UniversalClient, err error) goredis.UniversalClient {
			return consumerLifecycleErrorClient{UniversalClient: client, destroyErr: err}
		}},
		{name: "ensure", wrap: func(client goredis.UniversalClient, err error) goredis.UniversalClient {
			return consumerLifecycleErrorClient{UniversalClient: client, ensureErr: err}
		}},
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			ctx := context.Background()
			server := miniredis.RunT(t)
			base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
			oldConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
			newConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
			if err := NewEventBus(base, oldConsumer).EnsureConsumerGroup(ctx); err != nil {
				t.Fatalf("old EnsureConsumerGroup error: %v", err)
			}
			redisErr := errors.New("redis lifecycle failure")
			bus := NewEventBus(stage.wrap(base, redisErr), newConsumer)
			if err := bus.ReplaceConsumer(ctx, oldConsumer); !errors.Is(err, redisErr) {
				t.Fatalf("ReplaceConsumer error = %v, want injected error", err)
			}
			if !bus.IsDegraded() {
				t.Fatal("Redis error did not degrade bus")
			}
		})
	}
}

func TestRedisEventBusCloseConsumerGroupRedisErrorDegrades(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	redisErr := errors.New("redis close failure")
	bus := NewEventBus(consumerLifecycleErrorClient{UniversalClient: base, destroyErr: redisErr}, consumer)
	if err := bus.Close(ctx); !errors.Is(err, redisErr) {
		t.Fatalf("Close error = %v, want injected error", err)
	}
	if !bus.IsDegraded() {
		t.Fatal("Close Redis error did not degrade bus")
	}
}

func TestRedisEventBusCloseConsumerGroupRejectsInvalidCurrentConsumer(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	if err := client.XGroupCreateMkStream(ctx, EventsStreamKey(), "arbitrary-group", "$").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream error: %v", err)
	}
	bus := NewEventBus(client, StreamConsumer{
		NodeIdentity: "game/default/game-1", NodeSessionID: "session-a", Group: "arbitrary-group",
	})
	if err := bus.Close(ctx); !errors.Is(err, ErrSharedConsumerGroup) {
		t.Fatalf("Close error = %v, want ErrSharedConsumerGroup", err)
	}
	groups, err := client.XInfoGroups(ctx, bus.StreamKey()).Result()
	if err != nil {
		t.Fatalf("XInfoGroups error: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != "arbitrary-group" {
		t.Fatalf("groups after rejected close = %+v, want arbitrary group preserved", groups)
	}
}

func TestRedisEventBusConsumerLifecycleCancellationDoesNotDegrade(t *testing.T) {
	for _, lifecycleErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(lifecycleErr.Error(), func(t *testing.T) {
			server := miniredis.RunT(t)
			base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
			oldConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
			newConsumer, _ := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-b"})
			bus := NewEventBus(consumerLifecycleErrorClient{UniversalClient: base, pendingErr: lifecycleErr}, newConsumer)
			if err := bus.ReplaceConsumer(context.Background(), oldConsumer); !errors.Is(err, lifecycleErr) {
				t.Fatalf("ReplaceConsumer error = %v, want %v", err, lifecycleErr)
			}
			if bus.IsDegraded() {
				t.Fatal("ReplaceConsumer cancellation degraded bus")
			}

			bus = NewEventBus(consumerLifecycleErrorClient{UniversalClient: base, destroyErr: lifecycleErr}, newConsumer)
			if err := bus.Close(context.Background()); !errors.Is(err, lifecycleErr) {
				t.Fatalf("Close error = %v, want %v", err, lifecycleErr)
			}
			if bus.IsDegraded() {
				t.Fatal("Close cancellation degraded bus")
			}
		})
	}
}

func TestRedisEventBusPendingTimeoutEntersDegraded(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementReleased, GrainKey: sp.GrainKey("Player/10001")}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	handlerErr := errors.New("leave pending")
	if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error { return handlerErr }); !errors.Is(err, handlerErr) {
		t.Fatalf("Subscribe err = %v", err)
	}

	checkBus := NewEventBus(client, consumer)
	if err := checkBus.CheckPending(ctx, 0); err == nil {
		t.Fatal("CheckPending succeeded, want pending timeout error")
	}
	if !checkBus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}

func TestRedisEventBusCheckPendingCancellationDoesNotDegrade(t *testing.T) {
	for _, checkErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(checkErr.Error(), func(t *testing.T) {
			server := miniredis.RunT(t)
			base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
			consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
			if err != nil {
				t.Fatal(err)
			}
			bus := NewEventBus(consumerLifecycleErrorClient{UniversalClient: base, pendingExtErr: checkErr}, consumer)
			if err := bus.CheckPending(context.Background(), time.Second); !errors.Is(err, checkErr) {
				t.Fatalf("CheckPending error = %v, want %v", err, checkErr)
			}
			if bus.IsDegraded() {
				t.Fatal("CheckPending cancellation degraded bus")
			}
		})
	}
}

func TestRedisEventBusTrimKeepsPendingMessages(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementReleased, GrainKey: sp.GrainKey("Player/10001")}); err != nil {
		t.Fatalf("Publish first error: %v", err)
	}
	if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error { return errors.New("leave pending") }); err == nil {
		t.Fatal("Subscribe succeeded, want handler error")
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementTransferred, GrainKey: sp.GrainKey("Player/10002")}); err != nil {
		t.Fatalf("Publish second error: %v", err)
	}

	if err := bus.Trim(ctx, 1); err != nil {
		t.Fatalf("Trim error: %v", err)
	}
	messages, err := client.XRange(ctx, EventsStreamKey(), "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange error: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("stream len after safe trim = %d, want 2", len(messages))
	}
}

func TestRedisEventBusTrimKeepsUndeliveredMessages(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
	}

	if err := bus.Trim(ctx, 1); err != nil {
		t.Fatalf("Trim error: %v", err)
	}
	length, err := client.XLen(ctx, EventsStreamKey()).Result()
	if err != nil {
		t.Fatalf("XLen error: %v", err)
	}
	if length != 2 {
		t.Fatalf("stream len after safe trim = %d, want 2", length)
	}
}

func TestRedisEventBusTrimAtomicallyKeepsNewPendingMessage(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(base, consumer)
	for i := 0; i < 2; i++ {
		if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
	}
	hooked := &trimRaceClient{UniversalClient: base}
	hooked.mutate = func(ctx context.Context) error {
		if err := base.XGroupCreate(ctx, bus.StreamKey(), consumer.Group, "0").Err(); err != nil {
			return err
		}
		return base.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group: consumer.Group, Consumer: consumer.NodeSessionID,
			Streams: []string{bus.StreamKey(), ">"}, Count: 1,
		}).Err()
	}
	bus.client = hooked

	if err := bus.Trim(ctx, 1); err != nil {
		t.Fatalf("Trim error: %v", err)
	}
	if length := base.XLen(ctx, bus.StreamKey()).Val(); length != 2 {
		t.Fatalf("stream len after pending race = %d, want 2", length)
	}
}

func TestRedisEventBusTrimAtomicallyKeepsNewUndeliveredMessage(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(base, consumer)
	for i := 0; i < 2; i++ {
		if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
	}
	hooked := &trimRaceClient{UniversalClient: base}
	hooked.mutate = func(ctx context.Context) error {
		if err := base.XGroupCreate(ctx, bus.StreamKey(), consumer.Group, "$").Err(); err != nil {
			return err
		}
		return base.XAdd(ctx, &goredis.XAddArgs{
			Stream: bus.StreamKey(), Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
		}).Err()
	}
	bus.client = hooked

	if err := bus.Trim(ctx, 1); err != nil {
		t.Fatalf("Trim error: %v", err)
	}
	if length := base.XLen(ctx, bus.StreamKey()).Val(); length != 3 {
		t.Fatalf("stream len after lag race = %d, want 3", length)
	}
}

func TestRedisEventBusTrimGapEntersDegraded(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementReleased, GrainKey: sp.GrainKey("Player/10001")}); err != nil {
		t.Fatalf("Publish first error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventPlacementTransferred, GrainKey: sp.GrainKey("Player/10002")}); err != nil {
		t.Fatalf("Publish second error: %v", err)
	}
	if err := client.XTrimMaxLen(ctx, EventsStreamKey(), 1).Err(); err != nil {
		t.Fatalf("force trim error: %v", err)
	}
	bus.client = continuityMetadataClient{
		UniversalClient: client, entriesAdded: 2, entriesRead: 0, lag: 1,
	}
	if err := bus.CheckContinuity(ctx); err == nil {
		t.Fatal("CheckContinuity succeeded, want trim gap error")
	}
	if !bus.IsDegraded() {
		t.Fatal("bus did not enter degraded mode")
	}
}

func TestRedisEventBusContinuityDetectsDeletedPendingAtMaxDeleted(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(base, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	for _, id := range []string{"1000-0", "2000-0"} {
		if err := base.XAdd(ctx, &goredis.XAddArgs{
			Stream: bus.StreamKey(), ID: id, Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
		}).Err(); err != nil {
			t.Fatalf("XAdd %s error: %v", id, err)
		}
	}
	if err := base.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: consumer.Group, Consumer: consumer.NodeSessionID,
		Streams: []string{bus.StreamKey(), ">"}, Count: 2,
	}).Err(); err != nil {
		t.Fatalf("create pending error: %v", err)
	}
	if err := base.XDel(ctx, bus.StreamKey(), "1000-0").Err(); err != nil {
		t.Fatalf("XDel error: %v", err)
	}
	bus.client = continuityMetadataClient{
		UniversalClient: base, entriesAdded: 2, entriesRead: 2, lag: 0, maxDeletedID: "1000-0",
		pending: &goredis.XPending{Count: 2, Lower: "1000-0", Higher: "2000-0"},
		pendingExt: []goredis.XPendingExt{
			{ID: "1000-0", Consumer: consumer.NodeSessionID},
			{ID: "2000-0", Consumer: consumer.NodeSessionID},
		},
	}

	if err := bus.CheckContinuity(ctx); !errors.Is(err, ErrStreamGap) {
		t.Fatalf("CheckContinuity err = %v, want ErrStreamGap", err)
	}
}

func TestRedisEventBusContinuityDetectsPendingBeforeFirstEntry(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	for _, id := range []string{"1000-0", "2000-0"} {
		if err := client.XAdd(ctx, &goredis.XAddArgs{
			Stream: bus.StreamKey(), ID: id, Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
		}).Err(); err != nil {
			t.Fatalf("XAdd %s error: %v", id, err)
		}
	}
	if err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: consumer.Group, Consumer: consumer.NodeSessionID,
		Streams: []string{bus.StreamKey(), ">"}, Count: 1,
	}).Err(); err != nil {
		t.Fatalf("create pending error: %v", err)
	}
	if err := client.XTrimMaxLen(ctx, bus.StreamKey(), 1).Err(); err != nil {
		t.Fatalf("force trim error: %v", err)
	}
	bus.client = continuityMetadataClient{
		UniversalClient: client,
		pending:         &goredis.XPending{Count: 1, Lower: "1000-0", Higher: "1000-0"},
	}

	if err := bus.CheckContinuity(ctx); !errors.Is(err, ErrStreamGap) {
		t.Fatalf("CheckContinuity err = %v, want ErrStreamGap", err)
	}
}

func TestRedisEventBusContinuityDetectsPendingWhenStreamEmpty(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	if err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: consumer.Group, Consumer: consumer.NodeSessionID,
		Streams: []string{bus.StreamKey(), ">"}, Count: 1,
	}).Err(); err != nil {
		t.Fatalf("create pending error: %v", err)
	}
	if err := client.XTrimMaxLen(ctx, bus.StreamKey(), 0).Err(); err != nil {
		t.Fatalf("force trim error: %v", err)
	}
	bus.client = continuityMetadataClient{
		UniversalClient: client,
		pending:         &goredis.XPending{Count: 1, Lower: "1000-0", Higher: "1000-0"},
	}

	if err := bus.CheckContinuity(ctx); !errors.Is(err, ErrStreamGap) {
		t.Fatalf("CheckContinuity err = %v, want ErrStreamGap", err)
	}
}

func TestRedisEventBusContinuityUsesEntriesAddedForCrossMillisecondGap(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(base, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	for _, id := range []string{"1000-0", "2000-0"} {
		if err := base.XAdd(ctx, &goredis.XAddArgs{
			Stream: bus.StreamKey(), ID: id, Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
		}).Err(); err != nil {
			t.Fatalf("XAdd %s error: %v", id, err)
		}
	}
	if err := base.XTrimMaxLen(ctx, bus.StreamKey(), 1).Err(); err != nil {
		t.Fatalf("force trim error: %v", err)
	}
	bus.client = continuityMetadataClient{
		UniversalClient: base, entriesAdded: 2, entriesRead: 0, lag: 1,
	}

	if err := bus.CheckContinuity(ctx); !errors.Is(err, ErrStreamGap) {
		t.Fatalf("CheckContinuity err = %v, want ErrStreamGap", err)
	}
}

func TestRedisEventBusContinuityAllowsSafeHistoricalTrim(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(base, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	for _, id := range []string{"1000-0", "2000-0"} {
		if err := base.XAdd(ctx, &goredis.XAddArgs{
			Stream: bus.StreamKey(), ID: id, Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
		}).Err(); err != nil {
			t.Fatalf("XAdd %s error: %v", id, err)
		}
	}
	messages, err := base.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: consumer.Group, Consumer: consumer.NodeSessionID,
		Streams: []string{bus.StreamKey(), ">"}, Count: 2,
	}).Result()
	if err != nil {
		t.Fatalf("XReadGroup error: %v", err)
	}
	for _, message := range messages[0].Messages {
		if err := base.XAck(ctx, bus.StreamKey(), consumer.Group, message.ID).Err(); err != nil {
			t.Fatalf("XAck error: %v", err)
		}
	}
	if err := base.XTrimMaxLen(ctx, bus.StreamKey(), 1).Err(); err != nil {
		t.Fatalf("force trim error: %v", err)
	}
	bus.client = continuityMetadataClient{
		UniversalClient: base, entriesAdded: 2, entriesRead: 2, lag: 0,
	}

	if err := bus.CheckContinuity(ctx); err != nil {
		t.Fatalf("CheckContinuity error: %v", err)
	}
}

func TestRedisEventBusContinuityRetriesMixedStreamSnapshot(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(base, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	hooked := &continuityRaceClient{UniversalClient: base}
	hooked.mutate = func(ctx context.Context) error {
		return base.XAdd(ctx, &goredis.XAddArgs{
			Stream: bus.StreamKey(), Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
		}).Err()
	}
	bus.client = hooked

	if err := bus.CheckContinuity(ctx); err != nil {
		t.Fatalf("CheckContinuity error: %v", err)
	}
	if bus.IsDegraded() {
		t.Fatal("mixed stream snapshot put bus in degraded mode")
	}
}

func TestRedisEventBusContinuityBoundsRetriesOnActiveStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(base, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	hooked := &continuityRaceClient{UniversalClient: base, mutateEvery: true}
	hooked.mutate = func(ctx context.Context) error {
		return base.XAdd(ctx, &goredis.XAddArgs{
			Stream: bus.StreamKey(), Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
		}).Err()
	}
	bus.client = hooked

	if err := bus.CheckContinuity(ctx); !errors.Is(err, ErrStreamContinuityUnstable) {
		t.Fatalf("CheckContinuity err = %v, want ErrStreamContinuityUnstable", err)
	}
	if ctx.Err() != nil {
		t.Fatal("CheckContinuity retried until context timeout")
	}
	if bus.IsDegraded() {
		t.Fatal("active stream put bus in degraded mode")
	}
}

func TestRedisEventBusContinuityUnstableDoesNotHideGapOrConsume(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(base, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	hooked := &continuityRaceClient{
		UniversalClient: base, mutateEvery: true, entriesAddedOffset: 1,
	}
	hooked.mutate = func(ctx context.Context) error {
		return base.XAdd(ctx, &goredis.XAddArgs{
			Stream: bus.StreamKey(), Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
		}).Err()
	}
	bus.client = hooked

	if err := bus.CheckContinuity(ctx); !errors.Is(err, ErrStreamContinuityUnstable) {
		t.Fatalf("CheckContinuity err = %v, want ErrStreamContinuityUnstable", err)
	}
	handlerCalled := false
	if err := bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		handlerCalled = true
		return nil
	}); !errors.Is(err, ErrStreamContinuityUnstable) {
		t.Fatalf("Subscribe err = %v, want ErrStreamContinuityUnstable", err)
	}
	if handlerCalled {
		t.Fatal("handler called while continuity was unstable")
	}
	if bus.IsDegraded() {
		t.Fatal("unstable continuity put bus in degraded mode")
	}
}

func TestRedisEventBusPendingPayloadValidationUsesBoundedPipelineBatches(t *testing.T) {
	for _, missingSecondBatch := range []bool{false, true} {
		name := "all present"
		if missingSecondBatch {
			name = "missing in second batch"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			server := miniredis.RunT(t)
			base := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
			consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
			if err != nil {
				t.Fatalf("consumer error: %v", err)
			}
			bus := NewEventBus(base, consumer)
			entries := make([]goredis.XPendingExt, 101)
			for i := range entries {
				id := strconv.Itoa(1000+i) + "-0"
				entries[i] = goredis.XPendingExt{ID: id, Consumer: consumer.NodeSessionID}
				if err := base.XAdd(ctx, &goredis.XAddArgs{
					Stream: bus.StreamKey(), ID: id,
					Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
				}).Err(); err != nil {
					t.Fatalf("XAdd %s error: %v", id, err)
				}
			}
			if missingSecondBatch {
				if err := base.XDel(ctx, bus.StreamKey(), entries[100].ID).Err(); err != nil {
					t.Fatalf("XDel second batch error: %v", err)
				}
			}
			hooked := &pendingPayloadPipelineClient{UniversalClient: base, entries: entries}
			bus.client = hooked

			missing, err := bus.pendingPayloadMissing(ctx, int64(len(entries)))
			if err != nil {
				t.Fatalf("pendingPayloadMissing error: %v", err)
			}
			if missing != missingSecondBatch {
				t.Fatalf("missing = %v, want %v", missing, missingSecondBatch)
			}
			if fmt.Sprint(hooked.pendingCounts) != "[100 1]" {
				t.Fatalf("pending batch sizes = %v, want [100 1]", hooked.pendingCounts)
			}
			if fmt.Sprint(hooked.pipelineCounts) != "[100 1]" {
				t.Fatalf("pipeline batch sizes = %v, want [100 1]", hooked.pipelineCounts)
			}
		})
	}
}

func TestRedisEventBusRealRedisConsumerLifecycle(t *testing.T) {
	addr := os.Getenv("STABLE_PLACEMENT_REAL_REDIS_ADDR")
	if addr == "" {
		t.Skip("STABLE_PLACEMENT_REAL_REDIS_ADDR is not set")
	}
	client := goredis.NewClient(&goredis.Options{
		Addr: addr, Password: os.Getenv("STABLE_PLACEMENT_REAL_REDIS_PASSWORD"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("real Redis Ping error: %v", err)
	}

	newBuses := func(t *testing.T) (*EventBus, *EventBus, StreamConsumer) {
		t.Helper()
		oldConsumer, err := NewStreamConsumer(sp.Node{
			NodeIdentity: "game/default/task9", NodeSessionID: t.Name() + "-old",
		})
		if err != nil {
			t.Fatalf("old consumer error: %v", err)
		}
		newConsumer, err := NewStreamConsumer(sp.Node{
			NodeIdentity: "game/default/task9", NodeSessionID: t.Name() + "-new",
		})
		if err != nil {
			t.Fatalf("new consumer error: %v", err)
		}
		stream := fmt.Sprintf("sp:{stable-placement}:task9:%d", time.Now().UnixNano())
		oldBus := NewEventBus(client, oldConsumer)
		oldBus.stream = stream
		newBus := NewEventBus(client, newConsumer)
		newBus.stream = stream
		t.Cleanup(func() {
			if err := client.Del(context.Background(), stream).Err(); err != nil {
				t.Errorf("cleanup isolated stream error: %v", err)
			}
		})
		if err := oldBus.EnsureConsumerGroup(ctx); err != nil {
			t.Fatalf("old EnsureConsumerGroup error: %v", err)
		}
		return oldBus, newBus, oldConsumer
	}

	t.Run("replace retry and close", func(t *testing.T) {
		_, newBus, oldConsumer := newBuses(t)
		if err := newBus.ReplaceConsumer(ctx, oldConsumer); err != nil {
			t.Fatalf("ReplaceConsumer error: %v", err)
		}
		if err := newBus.ReplaceConsumer(ctx, oldConsumer); err != nil {
			t.Fatalf("ReplaceConsumer retry error: %v", err)
		}
		if err := newBus.Close(ctx); err != nil {
			t.Fatalf("Close error: %v", err)
		}
	})

	t.Run("lag blocks replacement", func(t *testing.T) {
		oldBus, newBus, oldConsumer := newBuses(t)
		if err := oldBus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
		if err := newBus.ReplaceConsumer(ctx, oldConsumer); !errors.Is(err, ErrPendingMessages) {
			t.Fatalf("ReplaceConsumer error = %v, want ErrPendingMessages", err)
		}
		groups, err := client.XInfoGroups(ctx, oldBus.stream).Result()
		if err != nil {
			t.Fatalf("XInfoGroups error: %v", err)
		}
		if len(groups) != 1 || groups[0].Name != oldConsumer.Group {
			t.Fatalf("groups after lag rejection = %+v, want old group only", groups)
		}
	})
}

func TestRedisEventBusRealRedisContinuityMetadata(t *testing.T) {
	addr := os.Getenv("STABLE_PLACEMENT_REAL_REDIS_ADDR")
	if addr == "" {
		t.Skip("STABLE_PLACEMENT_REAL_REDIS_ADDR is not set")
	}
	client := goredis.NewClient(&goredis.Options{
		Addr: addr, Password: os.Getenv("STABLE_PLACEMENT_REAL_REDIS_PASSWORD"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("real Redis Ping error: %v", err)
	}

	newBus := func(t *testing.T) (*EventBus, StreamConsumer) {
		t.Helper()
		consumer, err := NewStreamConsumer(sp.Node{
			NodeIdentity: "game/default/task8", NodeSessionID: t.Name(),
		})
		if err != nil {
			t.Fatalf("consumer error: %v", err)
		}
		bus := NewEventBus(client, consumer)
		bus.stream = fmt.Sprintf("sp:{stable-placement}:task8:%d", time.Now().UnixNano())
		t.Cleanup(func() {
			if err := client.Del(context.Background(), bus.stream).Err(); err != nil {
				t.Errorf("cleanup isolated stream error: %v", err)
			}
		})
		if err := bus.EnsureConsumerGroup(ctx); err != nil {
			t.Fatalf("EnsureConsumerGroup error: %v", err)
		}
		return bus, consumer
	}
	add := func(t *testing.T, bus *EventBus, id string) {
		t.Helper()
		if err := client.XAdd(ctx, &goredis.XAddArgs{
			Stream: bus.stream, ID: id, Values: eventValues(sp.PlacementEvent{Type: sp.EventManualCacheClear}),
		}).Err(); err != nil {
			t.Fatalf("XAdd %s error: %v", id, err)
		}
	}

	t.Run("cross millisecond undelivered gap", func(t *testing.T) {
		bus, _ := newBus(t)
		add(t, bus, "1000-0")
		add(t, bus, "2000-0")
		if err := client.XTrimMaxLen(ctx, bus.stream, 1).Err(); err != nil {
			t.Fatalf("XTrim error: %v", err)
		}
		if err := bus.CheckContinuity(ctx); !errors.Is(err, ErrStreamGap) {
			t.Fatalf("CheckContinuity err = %v, want ErrStreamGap", err)
		}
	})

	t.Run("safe historical trim", func(t *testing.T) {
		bus, consumer := newBus(t)
		add(t, bus, "1000-0")
		add(t, bus, "2000-0")
		streams, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group: consumer.Group, Consumer: consumer.NodeSessionID,
			Streams: []string{bus.stream, ">"}, Count: 2,
		}).Result()
		if err != nil {
			t.Fatalf("XReadGroup error: %v", err)
		}
		for _, message := range streams[0].Messages {
			if err := client.XAck(ctx, bus.stream, consumer.Group, message.ID).Err(); err != nil {
				t.Fatalf("XAck error: %v", err)
			}
		}
		if err := bus.Trim(ctx, 1); err != nil {
			t.Fatalf("Trim error: %v", err)
		}
		if length := client.XLen(ctx, bus.stream).Val(); length != 1 {
			t.Fatalf("stream len after safe trim = %d, want 1", length)
		}
		if err := bus.CheckContinuity(ctx); err != nil {
			t.Fatalf("CheckContinuity error: %v", err)
		}
	})

	t.Run("acked deletion between pending messages", func(t *testing.T) {
		bus, consumer := newBus(t)
		add(t, bus, "1000-0")
		add(t, bus, "2000-0")
		add(t, bus, "3000-0")
		streams, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group: consumer.Group, Consumer: consumer.NodeSessionID,
			Streams: []string{bus.stream, ">"}, Count: 3,
		}).Result()
		if err != nil {
			t.Fatalf("XReadGroup error: %v", err)
		}
		if len(streams) != 1 || len(streams[0].Messages) != 3 {
			t.Fatalf("messages = %+v, want 3", streams)
		}
		if err := client.XAck(ctx, bus.stream, consumer.Group, "2000-0").Err(); err != nil {
			t.Fatalf("XAck middle error: %v", err)
		}
		if err := client.XDel(ctx, bus.stream, "2000-0").Err(); err != nil {
			t.Fatalf("XDel middle error: %v", err)
		}

		if err := bus.CheckContinuity(ctx); err != nil {
			t.Fatalf("CheckContinuity error: %v", err)
		}
		if bus.IsDegraded() {
			t.Fatal("acked middle deletion put bus in degraded mode")
		}
	})

	t.Run("trimmed pending prefix", func(t *testing.T) {
		bus, consumer := newBus(t)
		add(t, bus, "1000-0")
		add(t, bus, "2000-0")
		if err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group: consumer.Group, Consumer: consumer.NodeSessionID,
			Streams: []string{bus.stream, ">"}, Count: 1,
		}).Err(); err != nil {
			t.Fatalf("XReadGroup error: %v", err)
		}
		if err := client.XTrimMaxLen(ctx, bus.stream, 1).Err(); err != nil {
			t.Fatalf("XTrim error: %v", err)
		}
		if err := bus.CheckContinuity(ctx); !errors.Is(err, ErrStreamGap) {
			t.Fatalf("CheckContinuity err = %v, want ErrStreamGap", err)
		}
	})

	t.Run("pending with empty stream", func(t *testing.T) {
		bus, consumer := newBus(t)
		add(t, bus, "1000-0")
		if err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group: consumer.Group, Consumer: consumer.NodeSessionID,
			Streams: []string{bus.stream, ">"}, Count: 1,
		}).Err(); err != nil {
			t.Fatalf("XReadGroup error: %v", err)
		}
		if err := client.XTrimMaxLen(ctx, bus.stream, 0).Err(); err != nil {
			t.Fatalf("XTrim error: %v", err)
		}
		if err := bus.CheckContinuity(ctx); !errors.Is(err, ErrStreamGap) {
			t.Fatalf("CheckContinuity err = %v, want ErrStreamGap", err)
		}
	})
}

func TestRedisEventBusContinuityAllowsUndeliveredMessages(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
	}

	if err := bus.CheckContinuity(ctx); err != nil {
		t.Fatalf("CheckContinuity error: %v", err)
	}
	if bus.IsDegraded() {
		t.Fatal("undelivered messages put bus in degraded mode")
	}
}

func TestRedisEventBusCanceledContinuityDoesNotDegrade(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = bus.CheckContinuity(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("CheckContinuity err = %v", err)
	}
	if bus.IsDegraded() {
		t.Fatal("canceled continuity check put bus in degraded mode")
	}
}

func TestRedisEventBusCanceledSubscribeDoesNotDegrade(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		t.Fatal("handler called for canceled subscription")
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe err = %v", err)
	}
	if bus.IsDegraded() {
		t.Fatal("canceled subscription put bus in degraded mode")
	}
}

func TestRedisEventBusContinuityAllowsNewGroupAtStreamEnd(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	for i := 0; i < 2; i++ {
		if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
	}
	if err := client.XTrimMaxLen(ctx, EventsStreamKey(), 1).Err(); err != nil {
		t.Fatalf("force trim error: %v", err)
	}
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}

	if err := bus.CheckContinuity(ctx); err != nil {
		t.Fatalf("CheckContinuity error: %v", err)
	}
	if bus.IsDegraded() {
		t.Fatal("new group at stream end put bus in degraded mode")
	}
}

func TestRedisEventBusContinuityAllowsMissingStreamOrGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.CheckContinuity(ctx); err != nil {
		t.Fatalf("CheckContinuity missing stream error: %v", err)
	}
	if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	if err := bus.CheckContinuity(ctx); err != nil {
		t.Fatalf("CheckContinuity missing group error: %v", err)
	}
	if bus.IsDegraded() {
		t.Fatal("missing stream or group put bus in degraded mode")
	}
}

func TestRedisEventBusSubscribeChecksContinuity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
	}
	if err := client.XTrimMaxLen(ctx, EventsStreamKey(), 1).Err(); err != nil {
		t.Fatalf("force trim error: %v", err)
	}
	bus.client = continuityMetadataClient{
		UniversalClient: client, entriesAdded: 2, entriesRead: 0, lag: 1,
	}

	handlerCalled := false
	err = bus.Subscribe(ctx, func(sp.PlacementEvent) error {
		handlerCalled = true
		return nil
	})
	if !errors.Is(err, ErrStreamGap) {
		t.Fatalf("Subscribe err = %v, want ErrStreamGap", err)
	}
	if handlerCalled {
		t.Fatal("handler called before continuity check")
	}
}

func TestRedisEventBusRunTrimLoopTrimsPeriodically(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	if err := bus.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup error: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := bus.Publish(ctx, sp.PlacementEvent{Type: sp.EventManualCacheClear}); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
	}
	if err := bus.DeleteConsumerGroup(ctx); err != nil {
		t.Fatalf("DeleteConsumerGroup error: %v", err)
	}
	trimCtx, trimCancel := context.WithCancel(ctx)
	defer trimCancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- bus.RunTrimLoop(trimCtx, 10*time.Millisecond, 1)
	}()

	for {
		length, err := client.XLen(ctx, EventsStreamKey()).Result()
		if err != nil {
			t.Fatalf("XLen error: %v", err)
		}
		if length == 1 {
			trimCancel()
			if err := <-errCh; err != nil {
				t.Fatalf("RunTrimLoop error: %v", err)
			}
			return
		}
		select {
		case err := <-errCh:
			t.Fatalf("RunTrimLoop returned early: %v", err)
		case <-ctx.Done():
			t.Fatalf("stream len after trim loop = %d, want 1", length)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRedisEventBusPubSubHintIsOptionalAndDoesNotWriteStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	consumer, err := NewStreamConsumer(sp.Node{NodeIdentity: "game/default/game-1", NodeSessionID: "session-a"})
	if err != nil {
		t.Fatalf("consumer error: %v", err)
	}
	bus := NewEventBus(client, consumer)
	got := make(chan sp.PlacementEvent, 1)
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- bus.SubscribeHint(subCtx, func(event sp.PlacementEvent) error {
			select {
			case got <- event:
			default:
			}
			subCancel()
			return nil
		})
	}()

	event := sp.PlacementEvent{Type: sp.EventPlacementTransferred, GrainKey: sp.GrainKey("Player/10001")}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case received := <-got:
			if received.Type != event.Type || received.GrainKey != event.GrainKey {
				t.Fatalf("hint event = %+v", received)
			}
			if err := <-errCh; err != nil {
				t.Fatalf("SubscribeHint error: %v", err)
			}
			length, err := client.XLen(ctx, EventsStreamKey()).Result()
			if err != nil {
				t.Fatalf("XLen error: %v", err)
			}
			if length != 0 {
				t.Fatalf("PublishHint wrote stream len = %d, want 0", length)
			}
			return
		case <-ticker.C:
			if err := bus.PublishHint(ctx, event); err != nil {
				t.Fatalf("PublishHint error: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("hint subscriber did not receive event")
		}
	}
}
