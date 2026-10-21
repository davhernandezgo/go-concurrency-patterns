package pipeline

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestPipeline_MapFilterReduce(t *testing.T) {
	ctx := context.Background()

	// Pipeline: Numbers 1..10 -> Filter Even -> Multiply by 10 -> Sum
	source := Generator(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	evens := Filter(ctx, source, func(n int) bool {
		return n%2 == 0
	})

	multiplied := Map(ctx, evens, func(ctx context.Context, n int) int {
		return n * 10
	})

	total := Reduce(ctx, multiplied, 0, func(acc int, item int) int {
		return acc + item
	})

	// Evens: 2, 4, 6, 8, 10
	// Multiplied: 20, 40, 60, 80, 100
	// Total: 300
	expected := 300
	if total != expected {
		t.Fatalf("expected %d, got %d", expected, total)
	}
}

func TestPipeline_Batch(t *testing.T) {
	ctx := context.Background()
	source := Generator(ctx, 1, 2, 3, 4, 5, 6, 7)

	batchesCh := Batch(ctx, source, 3, 50*time.Millisecond)

	var batches [][]int
	for b := range batchesCh {
		batches = append(batches, b)
	}

	expected := [][]int{
		{1, 2, 3},
		{4, 5, 6},
		{7},
	}

	if !reflect.DeepEqual(batches, expected) {
		t.Fatalf("expected %v, got %v", expected, batches)
	}
}

func TestPipeline_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// infinite-like generator
	numbers := make(chan int)
	go func() {
		defer close(numbers)
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case numbers <- i:
			}
		}
	}()

	mapped := Map(ctx, numbers, func(ctx context.Context, n int) int {
		return n * 2
	})

	// Read 3 items then cancel
	for i := 0; i < 3; i++ {
		<-mapped
	}
	cancel()

	// Drain mapped; it must close cleanly
	done := make(chan struct{})
	go func() {
		for range mapped {
		}
		close(done)
	}()

	select {
	case <-done:
		// success: pipeline terminated
	case <-time.After(1 * time.Second):
		t.Fatal("pipeline failed to terminate within timeout after context cancellation")
	}
}
