package bulkhead

import (
	"context"
	"testing"
)

func BenchmarkBulkhead_Execute_UnderLimit(b *testing.B) {
	bh := New[int](Config{
		MaxConcurrent: 100,
		MaxWaitQueue:  1000,
	})
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = bh.Execute(ctx, func(ctx context.Context) (int, error) {
				return 42, nil
			})
		}
	})
}

func BenchmarkBulkhead_TryExecute(b *testing.B) {
	bh := New[int](Config{
		MaxConcurrent: 100,
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = bh.TryExecute(func() (int, error) {
				return 42, nil
			})
		}
	})
}
