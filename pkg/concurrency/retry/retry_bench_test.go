package retry

import (
	"context"
	"testing"
	"time"
)

func BenchmarkRetry_ImmediateSuccess(b *testing.B) {
	ctx := context.Background()
	retrier := New[int](WithMaxAttempts(3))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = retrier.Execute(ctx, func(ctx context.Context) (int, error) {
				return 42, nil
			})
		}
	})
}

func BenchmarkRetry_TransientWithJitter(b *testing.B) {
	ctx := context.Background()
	retrier := New[int](
		WithMaxAttempts(2),
		WithInitialInterval(time.Microsecond),
		WithMaxInterval(10*time.Microsecond),
		WithJitter(FullJitter),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		attempt := 0
		_, _ = retrier.Execute(ctx, func(ctx context.Context) (int, error) {
			attempt++
			if attempt == 1 {
				return 0, errTransient
			}
			return 42, nil
		})
	}
}
