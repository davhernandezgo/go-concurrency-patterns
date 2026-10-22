package pubsub

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkPubSub_Publish_SingleSub(b *testing.B) {
	bus := New[int]()
	defer bus.Close()

	sub, _ := bus.Subscribe("bench.single", b.N+100, Block)
	defer sub.Unsubscribe()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = bus.Publish(ctx, "bench.single", i)
	}
}

func BenchmarkPubSub_Publish_FanOut_10Subs(b *testing.B) {
	bus := New[int]()
	defer bus.Close()

	const subCount = 10
	var subs []*Subscription[int]
	for i := 0; i < subCount; i++ {
		s, _ := bus.Subscribe("bench.fanout", b.N+100, DropOldest)
		subs = append(subs, s)
	}
	defer func() {
		for _, s := range subs {
			s.Unsubscribe()
		}
	}()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = bus.Publish(ctx, "bench.fanout", i)
	}
}

func BenchmarkPubSub_Publish_DropOldest_Contention(b *testing.B) {
	bus := New[int]()
	defer bus.Close()

	// Tight buffer to force constant drop-oldest ring buffer rotations
	sub, _ := bus.Subscribe("bench.drop", 16, DropOldest)
	defer sub.Unsubscribe()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = bus.Publish(ctx, "bench.drop", i)
	}
}

func BenchmarkPubSub_Publish_Wildcard(b *testing.B) {
	bus := New[string]()
	defer bus.Close()

	// Set up hierarchy
	for i := 0; i < 5; i++ {
		_, _ = bus.Subscribe(fmt.Sprintf("system.service%d.*", i), b.N+100, DropOldest)
	}
	_, _ = bus.Subscribe("*", b.N+100, DropOldest)

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = bus.Publish(ctx, "system.service0.metric", "payload")
	}
}
