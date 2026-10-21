package fanout

import (
	"context"
	"sort"
	"testing"
	"time"
)

func TestFanOutFanIn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	input := make(chan int, 20)
	for i := 1; i <= 20; i++ {
		input <- i
	}
	close(input)

	// Fan out across 4 workers
	workerFn := func(ctx context.Context, n int) int {
		// simulate processing time
		time.Sleep(5 * time.Millisecond)
		return n * n
	}

	workerChannels := FanOut(ctx, input, 4, workerFn)
	merged := FanIn(ctx, workerChannels...)

	var results []int
	for val := range merged {
		results = append(results, val)
	}

	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}

	sort.Ints(results)
	for i := 1; i <= 20; i++ {
		expected := i * i
		if results[i-1] != expected {
			t.Fatalf("at index %d: expected %d, got %d", i-1, expected, results[i-1])
		}
	}
}
