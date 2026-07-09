package redis

import (
	"encoding/base64"
	"strings"

	sp "github.com/wxdqing/stable-placement"
)

const hashTag = "{stable-placement}"

func PlacementKey(key sp.GrainKey) string {
	return "sp:" + hashTag + ":placement:" + encode(key.String())
}

func PlacementNodeKey(nodeIdentity string) string {
	return "sp:" + hashTag + ":placement_node:" + encode(nodeIdentity)
}

func NodesKey(nodeType string, nodeGroup string) string {
	return "sp:" + hashTag + ":nodes:" + encode(nodeType) + ":" + encode(nodeGroup)
}

func NodeKey(nodeIdentity string) string {
	return "sp:" + hashTag + ":node:" + encode(nodeIdentity)
}

func InvalidNodesKey(nodeType string, nodeGroup string) string {
	return "sp:" + hashTag + ":invalid_nodes:" + encode(nodeType) + ":" + encode(nodeGroup)
}

func EventsStreamKey() string {
	return "sp:" + hashTag + ":events:stream"
}

func EventsPubSubChannelKey() string {
	return "sp:" + hashTag + ":events:pubsub"
}

func AuditStreamKey() string {
	return "sp:" + hashTag + ":audit:stream"
}

func LeaseExpireKey() string {
	return "sp:" + hashTag + ":lease_expire"
}

func SequenceKey() string {
	return "sp:" + hashTag + ":seq"
}

func StrategyRoundRobinKey(nodeType string, nodeGroup string) string {
	return "sp:" + hashTag + ":strategy_rr:" + encode(nodeType) + ":" + encode(nodeGroup)
}

func ConsumerGroupName(nodeIdentity string, nodeSessionID string) string {
	return "sp:" + encode(nodeIdentity) + ":" + encode(nodeSessionID)
}

func HasStablePlacementHashTag(key string) bool {
	return strings.Contains(key, hashTag)
}

func encode(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
