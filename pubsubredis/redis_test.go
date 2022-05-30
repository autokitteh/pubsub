package pubsubredis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/autokitteh/pubsub"
)

func newTest(t *testing.T) pubsub.PubSub {
	r := miniredis.RunT(t)

	c := redis.NewClient(&redis.Options{
		Addr: r.Addr(),
	})

	return New(c)
}

func TestSingle(t *testing.T) {
	p := newTest(t)

	s, err := p.Subscribe(context.Background(), "test")
	if !assert.NoError(t, err) {
		return
	}

	payload := []byte("meow")

	if !assert.NoError(t, p.Publish(context.Background(), "test", payload)) {
		return
	}

	payload[0] = 'x'

	payload1, err := s.Consume(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, []byte("meow"), payload1)
}

func TestMulti(t *testing.T) {
	p := newTest(t)

	s, err := p.Subscribe(context.Background(), "test")
	if !assert.NoError(t, err) {
		return
	}

	if !assert.NoError(t, p.Publish(context.Background(), "test", []byte("meow"))) {
		return
	}

	if !assert.NoError(t, p.Publish(context.Background(), "test", []byte("woof"))) {
		return
	}

	payload1, err := s.Consume(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, []byte("meow"), payload1)

	payload2, err := s.Consume(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, []byte("woof"), payload2)

}

func TestUnsubscribe(t *testing.T) {
	p := newTest(t)

	s, err := p.Subscribe(context.Background(), "test")
	if !assert.NoError(t, err) {
		return
	}

	if !assert.NoError(t, p.Publish(context.Background(), "test", []byte("meow"))) {
		return
	}

	payload1, err := s.Consume(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, []byte("meow"), payload1)

	if !assert.NoError(t, s.Unsubscribe(context.Background())) {
		return
	}

	if !assert.NoError(t, p.Publish(context.Background(), "test", []byte("meow"))) {
		return
	}

	_, err = s.Consume(context.Background())
	assert.Equal(t, pubsub.ErrUnsubscribed, err)
}
