package redis

import (
	"errors"

	sp "github.com/wxdqing/stable-placement"
)

var ErrSharedConsumerGroup = errors.New("redis stream consumer group must be unique per node session")
var ErrPendingMessages = errors.New("redis stream has pending messages over threshold")
var ErrStreamGap = errors.New("redis stream has a trim gap")

type StreamConsumer struct {
	NodeIdentity  string
	NodeSessionID string
	Group         string
}

func NewStreamConsumer(node sp.Node) (StreamConsumer, error) {
	group := ConsumerGroupName(node.NodeIdentity, node.NodeSessionID)
	if node.NodeIdentity == "" || node.NodeSessionID == "" {
		return StreamConsumer{}, ErrSharedConsumerGroup
	}
	return StreamConsumer{
		NodeIdentity:  node.NodeIdentity,
		NodeSessionID: node.NodeSessionID,
		Group:         group,
	}, nil
}
