package pipeline

import (
	"context"
	"time"
)

// Generator takes a slice of inputs and streams them into an output channel until context cancellation.
func Generator[T any](ctx context.Context, items ...T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, item := range items {
			select {
			case <-ctx.Done():
				return
			case out <- item:
			}
		}
	}()
	return out
}

// Map applies a transform function fn to every element received from the in channel.
func Map[T any, R any](ctx context.Context, in <-chan T, fn func(context.Context, T) R) <-chan R {
	out := make(chan R)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				val := fn(ctx, item)
				select {
				case <-ctx.Done():
					return
				case out <- val:
				}
			}
		}
	}()
	return out
}

// Filter filters elements from in channel that do not satisfy the predicate.
func Filter[T any](ctx context.Context, in <-chan T, predicate func(T) bool) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				if predicate(item) {
					select {
					case <-ctx.Done():
						return
					case out <- item:
					}
				}
			}
		}
	}()
	return out
}

// Batch groups items into batches of up to batchSize, or flushes when maxWait duration elapses.
func Batch[T any](ctx context.Context, in <-chan T, batchSize int, maxWait time.Duration) <-chan []T {
	out := make(chan []T)
	go func() {
		defer close(out)
		batch := make([]T, 0, batchSize)
		ticker := time.NewTicker(maxWait)
		defer ticker.Stop()

		flush := func() {
			if len(batch) == 0 {
				return
			}
			toSend := batch
			batch = make([]T, 0, batchSize)
			select {
			case <-ctx.Done():
				return
			case out <- toSend:
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				flush()
			case item, ok := <-in:
				if !ok {
					flush()
					return
				}
				batch = append(batch, item)
				if len(batch) >= batchSize {
					flush()
					ticker.Reset(maxWait)
				}
			}
		}
	}()
	return out
}

// Reduce consumes the in channel and aggregates elements into a single result.
func Reduce[T any, R any](ctx context.Context, in <-chan T, initial R, fn func(acc R, item T) R) R {
	acc := initial
	for {
		select {
		case <-ctx.Done():
			return acc
		case item, ok := <-in:
			if !ok {
				return acc
			}
			acc = fn(acc, item)
		}
	}
}
