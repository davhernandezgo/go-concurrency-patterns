package fanout

import (
	"context"
	"sync"
)

// FanOut takes an input channel and distributes processing across numWorkers goroutines running workerFn.
// It returns a slice of channels, each fed by one worker.
func FanOut[T any, R any](ctx context.Context, in <-chan T, numWorkers int, workerFn func(context.Context, T) R) []<-chan R {
	channels := make([]<-chan R, numWorkers)
	for i := 0; i < numWorkers; i++ {
		out := make(chan R)
		channels[i] = out
		go func(ch chan<- R) {
			defer close(ch)
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						return
					}
					val := workerFn(ctx, item)
					select {
					case <-ctx.Done():
						return
					case ch <- val:
					}
				}
			}
		}(out)
	}
	return channels
}

// FanIn consolidates multiple input channels into a single unified output channel.
func FanIn[R any](ctx context.Context, channels ...<-chan R) <-chan R {
	out := make(chan R)
	var wg sync.WaitGroup

	for _, ch := range channels {
		wg.Add(1)
		go func(c <-chan R) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-c:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case out <- item:
					}
				}
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
