package redis

type EventBus struct {
	streamKey string
}

func NewEventBus() *EventBus {
	return &EventBus{streamKey: EventsStreamKey()}
}

func (b *EventBus) StreamKey() string {
	return b.streamKey
}
