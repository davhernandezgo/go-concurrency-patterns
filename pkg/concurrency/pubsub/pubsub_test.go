package pubsub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPubSub_ExactTopic(t *testing.T) {
	bus := New[string]()
	defer bus.Close()

	sub, err := bus.Subscribe("orders.created", 10, Block)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	if bus.SubscriberCount() != 1 {
		t.Fatalf("expected 1 subscriber, got %d", bus.SubscriberCount())
	}

	ctx := context.Background()
	if err := bus.Publish(ctx, "orders.created", "order-101"); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg := <-sub.C():
		if msg != "order-101" {
			t.Fatalf("expected order-101, got %s", msg)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for message")
	}
}

func TestPubSub_WildcardMatching(t *testing.T) {
	bus := New[string]()
	defer bus.Close()

	subExact, err := bus.Subscribe("order.created", 10, Block)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer subExact.Unsubscribe()

	subPrefix, err := bus.Subscribe("order.*", 10, Block)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer subPrefix.Unsubscribe()

	subAll, err := bus.Subscribe("*", 10, Block)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer subAll.Unsubscribe()

	ctx := context.Background()

	// 1. Publish matching all 3
	if err := bus.Publish(ctx, "order.created", "event-order-created"); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	for name, sub := range map[string]*Subscription[string]{
		"subExact":  subExact,
		"subPrefix": subPrefix,
		"subAll":    subAll,
	} {
		select {
		case msg := <-sub.C():
			if msg != "event-order-created" {
				t.Fatalf("[%s] expected event-order-created, got %s", name, msg)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("[%s] timed out waiting for message", name)
		}
	}

	// 2. Publish matching prefix and all, but NOT exact
	if err := bus.Publish(ctx, "order.shipped", "event-order-shipped"); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Verify exact sub did not receive
	select {
	case msg := <-subExact.C():
		t.Fatalf("exact subscriber should not receive order.shipped, got %s", msg)
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	// Prefix and All should receive
	for name, sub := range map[string]*Subscription[string]{
		"subPrefix": subPrefix,
		"subAll":    subAll,
	} {
		select {
		case msg := <-sub.C():
			if msg != "event-order-shipped" {
				t.Fatalf("[%s] expected event-order-shipped, got %s", name, msg)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("[%s] timed out waiting for message", name)
		}
	}

	// 3. Publish matching only all
	if err := bus.Publish(ctx, "user.registered", "event-user-registered"); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg := <-subAll.C():
		if msg != "event-user-registered" {
			t.Fatalf("expected event-user-registered, got %s", msg)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for all subscriber")
	}
}

func TestPubSub_Backpressure_DropOldest(t *testing.T) {
	bus := New[int]()
	defer bus.Close()

	// Buffer capacity 2
	sub, err := bus.Subscribe("telemetry", 2, DropOldest)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	ctx := context.Background()
	// Publish 5 messages without reading
	for i := 1; i <= 5; i++ {
		if err := bus.Publish(ctx, "telemetry", i); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}

	if sub.Dropped() != 3 {
		t.Fatalf("expected 3 dropped messages, got %d", sub.Dropped())
	}

	// Channel buffer should hold the last 2 messages (4 and 5)
	first := <-sub.C()
	second := <-sub.C()
	if first != 4 || second != 5 {
		t.Fatalf("expected [4, 5], got [%d, %d]", first, second)
	}
}

func TestPubSub_Backpressure_DropNewest(t *testing.T) {
	bus := New[int]()
	defer bus.Close()

	// Buffer capacity 2
	sub, err := bus.Subscribe("telemetry", 2, DropNewest)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	ctx := context.Background()
	// Publish 5 messages without reading
	for i := 1; i <= 5; i++ {
		if err := bus.Publish(ctx, "telemetry", i); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}

	if sub.Dropped() != 3 {
		t.Fatalf("expected 3 dropped messages, got %d", sub.Dropped())
	}

	// Channel buffer should hold the first 2 messages (1 and 2)
	first := <-sub.C()
	second := <-sub.C()
	if first != 1 || second != 2 {
		t.Fatalf("expected [1, 2], got [%d, %d]", first, second)
	}
}

func TestPubSub_Backpressure_Block_ContextCancel(t *testing.T) {
	bus := New[int]()
	defer bus.Close()

	// Buffer capacity 1
	sub, err := bus.Subscribe("queue", 1, Block)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	ctx := context.Background()
	// First message fills buffer
	if err := bus.Publish(ctx, "queue", 42); err != nil {
		t.Fatalf("first publish failed: %v", err)
	}

	// Second message with cancelled context should fail with context error
	cancelCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()

	err = bus.Publish(cancelCtx, "queue", 43)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded error, got %v", err)
	}
}

func TestPubSub_CloseGraceful(t *testing.T) {
	bus := New[string]()

	sub, err := bus.Subscribe("events", 5, Block)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	ctx := context.Background()
	_ = bus.Publish(ctx, "events", "msg-before-close")

	if err := bus.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Subsequent publish should fail with ErrBusClosed
	err = bus.Publish(ctx, "events", "msg-after-close")
	if !errors.Is(err, ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}

	// Subsequent subscribe should fail with ErrBusClosed
	_, err = bus.Subscribe("new.topic", 5, Block)
	if !errors.Is(err, ErrBusClosed) {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}

	// Subscriber channel should be closed after draining
	msg, ok := <-sub.C()
	if !ok || msg != "msg-before-close" {
		t.Fatalf("expected to read msg-before-close, got %s (ok=%v)", msg, ok)
	}

	// Next read should detect closed channel
	_, ok = <-sub.C()
	if ok {
		t.Fatal("expected subscriber channel to be closed after bus Close")
	}
}

func TestPubSub_ConcurrentPublishSubscribe(t *testing.T) {
	bus := New[int]()
	defer bus.Close()

	const numSubscribers = 10
	const numPublishers = 10
	const messagesPerPublisher = 50

	var subs []*Subscription[int]
	for i := 0; i < numSubscribers; i++ {
		sub, err := bus.Subscribe("concurrent.topic", 1000, DropOldest)
		if err != nil {
			t.Fatalf("subscribe failed: %v", err)
		}
		subs = append(subs, sub)
	}

	var pubWg sync.WaitGroup
	ctx := context.Background()

	// Concurrent publishers
	for p := 0; p < numPublishers; p++ {
		pubWg.Add(1)
		go func(pubID int) {
			defer pubWg.Done()
			for m := 0; m < messagesPerPublisher; m++ {
				_ = bus.Publish(ctx, "concurrent.topic", pubID*1000+m)
			}
		}(p)
	}

	// Concurrent unsubscribes of half the subscribers midway
	go func() {
		time.Sleep(5 * time.Millisecond)
		for i := 0; i < numSubscribers/2; i++ {
			subs[i].Unsubscribe()
		}
	}()

	pubWg.Wait()

	// Clean unsubscribe for the rest
	for i := numSubscribers / 2; i < numSubscribers; i++ {
		subs[i].Unsubscribe()
	}
}

func TestPubSub_Validation(t *testing.T) {
	bus := New[string]()
	defer bus.Close()

	if _, err := bus.Subscribe("", 5, Block); !errors.Is(err, ErrTopicEmpty) {
		t.Fatalf("expected ErrTopicEmpty, got %v", err)
	}

	if err := bus.Publish(context.Background(), "", "data"); !errors.Is(err, ErrTopicEmpty) {
		t.Fatalf("expected ErrTopicEmpty, got %v", err)
	}
}
