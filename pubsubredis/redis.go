package pubsubredis

import (
	"context"

	"github.com/go-redis/redis/v8"

	"github.com/autokitteh/pubsub"
)

type subscriber struct {
	sub *redis.PubSub
}

type pubsubredis struct {
	client *redis.Client
}

var _ pubsub.PubSub = &pubsubredis{}

func New(client *redis.Client) pubsub.PubSub {
	return &pubsubredis{client: client}
}

func (r *pubsubredis) Subscribe(ctx context.Context, topic string) (pubsub.Subscriber, error) {
	sub := r.client.Subscribe(ctx, topic)

	_, err := sub.Receive(ctx)
	if err != nil {
		return nil, err
	}

	return &subscriber{sub: sub}, nil
}

func (r *pubsubredis) Publish(ctx context.Context, topic string, payload []byte) error {
	_, err := r.client.Publish(ctx, topic, payload).Result()
	return err
}

func (s *subscriber) Unsubscribe(_ context.Context) error {
	return s.sub.Close()
}

func (s *subscriber) Consume(ctx context.Context) ([]byte, error) {
	ch := s.sub.Channel()

	select {
	case x := <-ch:
		if x == nil {
			return nil, pubsub.ErrUnsubscribed
		}

		return []byte(x.Payload), nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
