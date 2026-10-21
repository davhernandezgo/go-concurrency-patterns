package workerpool

import (
	"context"
	"sync"
	"testing"
)

// BenchmarkWorkerPool measures performance of bounded worker pool
func BenchmarkWorkerPool(b *testing.B) {
	handler := func(ctx context.Context, n int) (int, error) {
		// simulate lightweight CPU-bound work
		sum := 0
		for i := 0; i < 50; i++ {
			sum += i * n
		}
		return sum, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool := New[int, int](8, 100, handler)
		pool.Start()

		go func() {
			for j := 0; j < 1000; j++ {
				_ = pool.Submit(context.Background(), j)
			}
			pool.Stop()
		}()

		for range pool.Results() {
		}
	}
}

// BenchmarkUnboundedGoroutines measures performance of spawning a new goroutine per task
func BenchmarkUnboundedGoroutines(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j := 0; j < 1000; j++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				sum := 0
				for k := 0; k < 50; k++ {
					sum += k * n
				}
				_ = sum
			}(j)
		}
		wg.Wait()
	}
}
