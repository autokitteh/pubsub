package pubsubinmem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingle(t *testing.T) {
	p := NewInMem()

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

	payload2, ack, err := s.Consume(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, []byte("meow"), payload2)

	ack()

	// double ack
	assert.Panics(t, ack)
}

func TestMulti(t *testing.T) {
	p := NewInMem()

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

func TestParallel(t *testing.T) {
}
