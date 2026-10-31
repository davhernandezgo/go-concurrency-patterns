package scattergather

import (
	"context"
	"testing"
)

func BenchmarkScatterGather_FastQuorum(b *testing.B) {
	engine := New[int](
		WithMinQuorum[int](2),
		WithFastExitOnQuorum[int](true),
	)
	ctx := context.Background()

	tasks := []Task[int]{
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (int, error) { return 2, nil },
		func(ctx context.Context) (int, error) { return 3, nil },
		func(ctx context.Context) (int, error) { return 4, nil },
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = engine.Execute(ctx, tasks...)
		}
	})
}

func BenchmarkScatterGather_AllComplete(b *testing.B) {
	engine := New[int](
		WithMinQuorum[int](4),
		WithFastExitOnQuorum[int](false),
	)
	ctx := context.Background()

	tasks := []Task[int]{
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (int, error) { return 2, nil },
		func(ctx context.Context) (int, error) { return 3, nil },
		func(ctx context.Context) (int, error) { return 4, nil },
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = engine.Execute(ctx, tasks...)
		}
	})
}

func BenchmarkScatterGather_Aggregator(b *testing.B) {
	ctx := context.Background()
	agg := func(results []TaskResult[int]) (int, error) {
		sum := 0
		for _, r := range results {
			sum += r.Value
		}
		return sum, nil
	}

	tasks := []Task[int]{
		func(ctx context.Context) (int, error) { return 10, nil },
		func(ctx context.Context) (int, error) { return 20, nil },
		func(ctx context.Context) (int, error) { return 30, nil },
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Gather(ctx, 3, agg, tasks...)
		}
	})
}
