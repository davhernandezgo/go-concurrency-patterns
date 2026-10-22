package pubsub

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// ErrBusClosed is returned when publishing or subscribing to a closed event bus.
	ErrBusClosed = errors.New("pubsub: event bus is closed")
	// ErrTopicEmpty is returned when an invalid empty topic is specified.
	ErrTopicEmpty = errors.New("pubsub: topic cannot be empty")
)

// BackpressureStrategy defines how to handle slow subscribers when their channel buffer is full.
type BackpressureStrategy int

const (
	// Block blocks the publisher until buffer space becomes available or context is canceled.
	Block BackpressureStrategy = iota
	// DropOldest drops the oldest message in the subscriber's buffer to make room for the new message.
	DropOldest
	// DropNewest discards the incoming message if the subscriber's buffer is full.
	DropNewest
)

// Subscription represents an active topic subscription.
type Subscription[T any] struct {
	topic    string
	ch       chan T
	strategy BackpressureStrategy
	bus      *EventBus[T]

	once       sync.Once
	closed     atomic.Bool
	closing    chan struct{}
	activePubs atomic.Int64
	dropped    atomic.Uint64
}

// C returns the read-only channel where events are received.
func (s *Subscription[T]) C() <-chan T {
	return s.ch
}

// Topic returns the subscription topic or topic pattern.
func (s *Subscription[T]) Topic() string {
	return s.topic
}

// Dropped returns the total number of messages dropped due to backpressure.
func (s *Subscription[T]) Dropped() uint64 {
	return s.dropped.Load()
}

// Unsubscribe removes the subscription from the bus and closes its channel cleanly.
// It is safe to call multiple times concurrently.
func (s *Subscription[T]) Unsubscribe() {
	s.once.Do(func() {
		s.closed.Store(true)
		close(s.closing)
		if s.bus != nil {
			s.bus.removeSubscription(s)
		}
		for s.activePubs.Load() > 0 {
			runtime.Gosched()
		}
		close(s.ch)
	})
}

func (s *Subscription[T]) send(ctx context.Context, msg T) error {
	if s.closed.Load() {
		return nil
	}

	s.activePubs.Add(1)
	defer s.activePubs.Add(-1)

	if s.closed.Load() {
		return nil
	}

	switch s.strategy {
	case DropNewest:
		select {
		case <-s.closing:
			return nil
		case s.ch <- msg:
		default:
			s.dropped.Add(1)
		}
		return nil

	case DropOldest:
		for {
			select {
			case <-s.closing:
				return nil
			case s.ch <- msg:
				return nil
			default:
				select {
				case <-s.closing:
					return nil
				case <-s.ch:
					s.dropped.Add(1)
				default:
				}
			}
		}

	case Block:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closing:
			return nil
		case s.ch <- msg:
			return nil
		}
	}
	return nil
}

// EventBus is a high-performance in-memory publish-subscribe message broker.
type EventBus[T any] struct {
	mu          sync.RWMutex
	subscribers map[string][]*Subscription[T]
	closed      bool
}

// New creates a new thread-safe generic EventBus.
func New[T any]() *EventBus[T] {
	return &EventBus[T]{
		subscribers: make(map[string][]*Subscription[T]),
	}
}

// Subscribe registers a new subscriber for a given topic or pattern.
// Supported patterns:
//   - Exact match: e.g. "order.created"
//   - Prefix wildcard: e.g. "order.*" (matches "order.created", "order.shipped")
//   - Global wildcard: "*" (matches all topics)
func (b *EventBus[T]) Subscribe(topic string, bufferSize int, strategy BackpressureStrategy) (*Subscription[T], error) {
	if topic == "" {
		return nil, ErrTopicEmpty
	}
	if bufferSize < 1 {
		bufferSize = 1
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBusClosed
	}

	sub := &Subscription[T]{
		topic:    topic,
		ch:       make(chan T, bufferSize),
		strategy: strategy,
		bus:      b,
		closing:  make(chan struct{}),
	}

	b.subscribers[topic] = append(b.subscribers[topic], sub)
	return sub, nil
}

// Publish broadcasts a message to all subscribers matching the specified topic.
// If any subscriber uses the Block strategy and ctx is canceled, Publish aborts and returns the context error.
func (b *EventBus[T]) Publish(ctx context.Context, topic string, msg T) error {
	if topic == "" {
		return ErrTopicEmpty
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrBusClosed
	}

	var matched []*Subscription[T]
	for pattern, subs := range b.subscribers {
		if topicMatches(pattern, topic) {
			matched = append(matched, subs...)
		}
	}
	b.mu.RUnlock()

	for _, sub := range matched {
		if err := sub.send(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// topicMatches checks if topic matches pattern.
func topicMatches(pattern, topic string) bool {
	if pattern == "*" || pattern == topic {
		return true
	}
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, ".*")
		if topic == prefix || strings.HasPrefix(topic, prefix+".") {
			return true
		}
	}
	return false
}

// SubscriberCount returns the total number of active subscribers across all topics.
func (b *EventBus[T]) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0
	for _, subs := range b.subscribers {
		count += len(subs)
	}
	return count
}

func (b *EventBus[T]) removeSubscription(sub *Subscription[T]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs, ok := b.subscribers[sub.topic]
	if !ok {
		return
	}

	filtered := make([]*Subscription[T], 0, len(subs))
	for _, s := range subs {
		if s != sub {
			filtered = append(filtered, s)
		}
	}

	if len(filtered) == 0 {
		delete(b.subscribers, sub.topic)
	} else {
		b.subscribers[sub.topic] = filtered
	}
}

// Close gracefully closes the event bus and unsubscribes all active subscribers.
func (b *EventBus[T]) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true

	// Gather all current subscriptions
	var allSubs []*Subscription[T]
	for _, subs := range b.subscribers {
		allSubs = append(allSubs, subs...)
	}
	b.subscribers = make(map[string][]*Subscription[T])
	b.mu.Unlock()

	// Unsubscribe all outside the bus lock to prevent deadlocks
	for _, sub := range allSubs {
		sub.Unsubscribe()
	}
	return nil
}
