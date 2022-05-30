package pubsub

import (
	"context"
	"errors"
)

var ErrUnsubscribed = errors.New("unsubscribed")

type Subscriber interface {
	Unsubscribe(context.Context) error

	// Returns ErrUnsubscribed if unsubscribed.
	Consume(context.Context) ([]byte, error)
}

type PubSub interface {
	Subscribe(_ context.Context, topic string) (Subscriber, error)
	Publish(_ context.Context, topic string, payload []byte) error
}
