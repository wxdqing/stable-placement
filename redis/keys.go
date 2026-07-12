package redis

import (
	"encoding/base64"
	"strings"

	sp "github.com/wxdqing/stable-placement"
)

const (
	hashTag          = "{stable-placement}"
	NamespaceVersion = "v2"
	NamespacePrefix  = "sp:" + hashTag + ":" + NamespaceVersion + ":"
)

func PlacementKey(key sp.GrainKey) string {
	return NamespacePrefix + "placement:" + encode(key.String())
}

func PlacementNodeKey(nodeIdentity string) string {
	return NamespacePrefix + "placement:node:" + encode(nodeIdentity)
}

func NodesKey(nodeType string, nodeGroup string) string {
	return NamespacePrefix + "nodes:" + encode(nodeType) + ":" + encode(nodeGroup)
}

func NodeKey(nodeIdentity string) string {
	return NamespacePrefix + "node:" + encode(nodeIdentity)
}

func InvalidNodesKey(nodeType string, nodeGroup string) string {
	return NamespacePrefix + "invalid:" + encode(nodeType) + ":" + encode(nodeGroup)
}

func NodeLeaseKey(nodeType string, nodeGroup string) string {
	return NamespacePrefix + "node_lease:" + encode(nodeType) + ":" + encode(nodeGroup)
}

func EventsStreamKey() string {
	return NamespacePrefix + "events:stream"
}

func EventsPubSubChannelKey() string {
	return NamespacePrefix + "events:pubsub"
}

func AuditStreamKey() string {
	return NamespacePrefix + "audit:stream"
}

func SequenceKey() string {
	return NamespacePrefix + "sequence"
}

func StrategyRoundRobinKey(nodeType string, nodeGroup string) string {
	return NamespacePrefix + "strategy:round_robin:" + encode(nodeType) + ":" + encode(nodeGroup)
}

func ConsumerGroupName(nodeIdentity string, nodeSessionID string) string {
	return "sp:" + NamespaceVersion + ":consumer:" + encode(nodeIdentity) + ":" + encode(nodeSessionID)
}

func HasStablePlacementHashTag(key string) bool {
	return strings.Contains(key, hashTag)
}

func encode(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
