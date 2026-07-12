package redis

import (
	"testing"

	sp "github.com/wxdqing/stable-placement"
)

func TestRedisEventBusNodeLeaseExpiredPayload(t *testing.T) {
	want := sp.PlacementEvent{Type: sp.EventNodeLeaseExpired, NodeIdentity: "game/default/game-1", NodeSessionID: "session-a", NodeLeaseVersion: 7}
	got, err := parseEvent(eventValues(want))
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.NodeIdentity != want.NodeIdentity || got.NodeSessionID != want.NodeSessionID || got.NodeLeaseVersion != want.NodeLeaseVersion {
		t.Fatalf("got = %+v", got)
	}
}

func TestRedisEventBusRejectsIncompleteNodeLeaseExpired(t *testing.T) {
	if _, err := parseEvent(map[string]any{"type": string(sp.EventNodeLeaseExpired), "node_identity": "game/default/game-1"}); err == nil {
		t.Fatal("expected malformed event error")
	}
}

func TestRedisEventBusPlacementPayloadHasNoGrainLeaseVersion(t *testing.T) {
	values := eventValues(sp.PlacementEvent{Type: sp.EventPlacementReleased, GrainKey: "Player/1", PlacementVersion: 2})
	if _, ok := values["lease_version"]; ok {
		t.Fatal("placement payload retained grain lease version")
	}
}
