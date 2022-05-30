package pubsubinmem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/autokitteh/pubsub"
)

func TestSingle(t *testing.T) {
	p := NewInMem(0)

	s, err := p.Subscribe(context.Background(), "test")
	if !assert.NoError(t, err) {
		return
	}

	payload := []byte("meow")

	if !assert.NoError(t, p.Publish(context.Background(), "test", payload)) {
		return
	}

	payload[0] = 'x'

	payload1, _, err := s.Consume(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, []byte("meow"), payload1)
}

func TestMulti(t *testing.T) {
	p := NewInMem(0)

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

	payload1, ack, err := s.Consume(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	ack()

	assert.Equal(t, []byte("meow"), payload1)

	payload2, ack, err := s.Consume(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, []byte("woof"), payload2)

	ack()
}

func TestUnsubscribe(t *testing.T) {
	p := NewInMem(0)

	s, err := p.Subscribe(context.Background(), "test")
	if !assert.NoError(t, err) {
		return
	}

	if !assert.NoError(t, p.Publish(context.Background(), "test", []byte("meow"))) {
		return
	}

	payload1, _, err := s.Consume(context.Background())
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

	_, _, err = s.Consume(context.Background())
	assert.Equal(t, pubsub.ErrUnsubscribed, err)
}

func TestUnsubscribeWithLeftovers(t *testing.T) {
	p := NewInMem(0)

	s, err := p.Subscribe(context.Background(), "test")
	if !assert.NoError(t, err) {
		return
	}

	if !assert.NoError(t, p.Publish(context.Background(), "test", []byte("meow"))) {
		return
	}

	if !assert.NoError(t, s.Unsubscribe(context.Background())) {
		return
	}

	payload1, _, err := s.Consume(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, []byte("meow"), payload1)

	_, _, err = s.Consume(context.Background())
	assert.Equal(t, pubsub.ErrUnsubscribed, err)
}
