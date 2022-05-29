package pubsubinmem

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/autokitteh/pubsub"
)

type unacked struct {
	id      int
	payload []byte
}

type subscriber struct {
	mu      sync.Mutex
	unacked *unacked
	q       chan []byte
	m       *inmem
	topic   string
}

type inmem struct {
	mu     sync.RWMutex
	topics map[string][]*subscriber
}

var _ pubsub.PubSub = &inmem{}

func NewInMem() pubsub.PubSub {
	return &inmem{topics: make(map[string][]*subscriber)}
}

func (m *inmem) Subscribe(_ context.Context, topic string) (pubsub.Subscriber, error) {
	s := &subscriber{topic: topic, q: make(chan []byte, 16), m: m}

	m.mu.Lock()
	m.topics[topic] = append(m.topics[topic], s)
	m.mu.Unlock()

	return s, nil
}

func (m *inmem) Publish(ctx context.Context, topic string, payload []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	xs := m.topics[topic]

	p := make([]byte, len(payload))
	copy(p, payload)

	wg, ctx1 := errgroup.WithContext(ctx)

	for _, x := range xs {
		x := x

		wg.Go(func() error {
			select {
			case x.q <- p:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-ctx1.Done():
				return ctx1.Err()
			}
		})
	}

	return wg.Wait()
}

func (s *subscriber) Unsubscribe(_ context.Context) error {
	s.mu.Lock()

	if s.m == nil {
		s.mu.Unlock()
		return nil
	}

	s.m = nil

	s.mu.Unlock()

	s.m.mu.Lock()
	defer s.m.mu.Unlock()

	xs := s.m.topics[s.topic]
	if xs == nil {
		panic("topic not found")
	}

	for i, x := range xs {
		if x == s {
			s.m.topics[s.topic] = append(xs[:i], xs[i+1:]...)
			s.m = nil
			return nil
		}
	}

	panic("subscriber not found in parent topic")
}

func (s *subscriber) Consume(ctx context.Context) (payload []byte, ack func(), err error) {
	s.mu.Lock()

	if unacked := s.unacked; unacked != nil {
		s.mu.Unlock()

		ack = func() {
			s.mu.Lock()
			defer s.mu.Unlock()

			if s.unacked != unacked || s.unacked.id != unacked.id {
				panic("already acked or corrupted")
			}

			s.unacked = nil
		}

		payload = s.unacked.payload
		return
	}

	select {
	case x := <-s.q:
		s.unacked = &unacked{
			payload: x,
			id:      0,
		}

		s.mu.Unlock()

		return s.Consume(ctx)

	case <-ctx.Done():
		s.mu.Unlock()

		err = ctx.Err()
	}

	return
}
