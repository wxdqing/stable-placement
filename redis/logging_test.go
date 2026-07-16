package redis

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

func TestEventBusLogsFirstDegradedCause(t *testing.T) {
	var output bytes.Buffer
	consumer := StreamConsumer{
		NodeIdentity: "game/default/game-1",
		NodeSessionID: "session-1",
		Group:         "group-1",
	}
	bus := NewEventBus(nil, consumer, WithLogger(sp.NewStdLogger(log.New(&output, "", 0))))

	bus.setDegraded("consume stream", errors.New("redis unavailable"))
	bus.setDegraded("ack stream event", errors.New("second error"))

	text := output.String()
	if strings.Count(text, "event bus degraded") != 1 ||
		!strings.Contains(text, "consume stream") ||
		!strings.Contains(text, "redis unavailable") ||
		strings.Contains(text, "second error") {
		t.Fatalf("logger output = %q", text)
	}
}

func TestEventBusDoesNotDegradeOnCancellation(t *testing.T) {
	var output bytes.Buffer
	bus := NewEventBus(nil, StreamConsumer{}, WithLogger(sp.NewStdLogger(log.New(&output, "", 0))))
	bus.setDegraded("consume stream", context.Canceled)
	if bus.IsDegraded() {
		t.Fatal("cancellation degraded event bus")
	}
	if output.Len() != 0 {
		t.Fatalf("logger output = %q", output.String())
	}
}
