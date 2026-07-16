package memory

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	sp "github.com/wxdqing/stable-placement"
	"github.com/wxdqing/stable-placement/strategies"
)

func TestDirectoryLogsBestEffortPublishFailure(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	publisher := &recordingPublisher{}
	registry := newTestRegistry(t, clock, publisher, time.Minute)
	node := testNode("game-1", "session-1")
	if _, err := registry.RegisterNode(context.Background(), node); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	directory, err := NewDirectory(
		registry,
		sp.StrategyModeGo,
		strategies.NewRoundRobin(),
		publisher,
		WithLogger(sp.NewStdLogger(log.New(&output, "", 0))),
	)
	if err != nil {
		t.Fatal(err)
	}
	publisher.err = errors.New("publish unavailable")

	placement, err := directory.Allocate(context.Background(), sp.AllocateCommand{
		GrainID: "10001", Kind: "Player", TargetNodeType: "game", TargetNodeGroup: "default",
	})
	if err != nil {
		t.Fatalf("Allocate error = %v", err)
	}
	text := output.String()
	if !strings.Contains(text, "PlacementCreated") ||
		!strings.Contains(text, placement.GrainKey.String()) ||
		!strings.Contains(text, "publish unavailable") {
		t.Fatalf("logger output = %q", text)
	}
}

func TestDirectoryDoesNotLogCanceledBestEffortPublish(t *testing.T) {
	var output bytes.Buffer
	directory := &Directory{
		publisher: &recordingPublisher{err: context.Canceled},
		logger:    sp.NewStdLogger(log.New(&output, "", 0)),
		registry:  &NodeRegistry{now: time.Now},
	}
	directory.publishBestEffort(context.Background(), sp.Placement{}, sp.EventPlacementCreated)
	if output.Len() != 0 {
		t.Fatalf("logger output = %q", output.String())
	}
}
