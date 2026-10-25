package hedged

import (
	"context"
	"testing"
	"time"
)

func BenchmarkHedged_PrimaryFast(b *testing.B) {
	engine := New[string](WithDelay(50 * time.Millisecond))
	ctx := context.Background()

	primary := func(ctx context.Context) (string, error) {
		return "fast-val", nil
	}
	hedge := func(ctx context.Context) (string, error) {
		return "hedge-val", nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = engine.Execute(ctx, primary, hedge)
	}
}

func BenchmarkHedged_ParallelRace(b *testing.B) {
	engine := New[int](WithDelay(0))
	ctx := context.Background()

	t1 := func(ctx context.Context) (int, error) {
		return 1, nil
	}
	t2 := func(ctx context.Context) (int, error) {
		return 2, nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = engine.Execute(ctx, t1, t2)
	}
}
