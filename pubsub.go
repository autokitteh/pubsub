package pubsub

import (
	"context"
)

type Subscriber interface {
	Unsubscribe(context.Context) error

	// Consume the next item. If previous item is unacked, it will be returned
	// again. Every item can be acked at most once. This is designed so each
	// subscriber is a single consumer.
	Consume(_ context.Context) (_ []byte, ack func(), _ error)
}

type PubSub interface {
	Subscribe(_ context.Context, topic string) (Subscriber, error)
	Publish(_ context.Context, topic string, payload []byte) error
}
