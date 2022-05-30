package pubsubinmem

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/autokitteh/pubsub"
)

const DefaultQueueSize = 32

type subscriber struct {
	q     chan []byte
	m     *inmem
	topic string
}

type inmem struct {
	mu     sync.RWMutex
	topics map[string][]*subscriber
	qSize  int
}

var _ pubsub.PubSub = &inmem{}

func NewInMem(qSize int) pubsub.PubSub {
	if qSize == 0 {
		qSize = DefaultQueueSize
	}

	return &inmem{topics: make(map[string][]*subscriber), qSize: qSize}
}

func (m *inmem) Subscribe(_ context.Context, topic string) (pubsub.Subscriber, error) {
	s := &subscriber{topic: topic, q: make(chan []byte, m.qSize), m: m}

	m.mu.Lock()
	m.topics[topic] = append(m.topics[topic], s)
	m.mu.Unlock()

	return s, nil
}

func (m *inmem) Publish(ctx context.Context, topic string, payload []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	xs := m.topics[topic]

	wg, wgctx := errgroup.WithContext(ctx)

	for _, x := range xs {
		p := make([]byte, len(payload))
		copy(p, payload)

		x := x

		wg.Go(func() error {
			select {
			case x.q <- p:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-wgctx.Done():
				return wgctx.Err()
			}
		})
	}

	return wg.Wait()
}

func (s *subscriber) Unsubscribe(_ context.Context) error {
	s.m.mu.Lock()
	defer s.m.mu.Unlock()

	close(s.q)

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
	select {
	case x := <-s.q:
		if x == nil {
			return nil, nil, pubsub.ErrUnsubscribed
		}

		return x, func() {}, nil

	case <-ctx.Done():
		err = ctx.Err()
	}

	return
}
