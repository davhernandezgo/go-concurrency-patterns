package bulkhead

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBulkhead_UnderCapacity(t *testing.T) {
	b := New[int](Config{
		MaxConcurrent: 5,
		MaxWaitQueue:  10,
	})

	ctx := context.Background()
	res, err := b.Execute(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != 42 {
		t.Fatalf("expected 42, got %d", res)
	}

	stats := b.Stats()
	if stats.TotalExecuted != 1 {
		t.Errorf("expected 1 executed, got %d", stats.TotalExecuted)
	}
	if stats.ActiveExecutions != 0 {
		t.Errorf("expected 0 active, got %d", stats.ActiveExecutions)
	}
}

func TestBulkhead_QueueBufferingAndExecution(t *testing.T) {
	b := New[string](Config{
		MaxConcurrent: 2,
		MaxWaitQueue:  5,
		QueueTimeout:  200 * time.Millisecond,
	})

	ctx := context.Background()
	startBlock := make(chan struct{})
	releaseBlock := make(chan struct{})

	// Launch 2 workers to fill the active slots
	for i := 0; i < 2; i++ {
		go func() {
			_, _ = b.Execute(ctx, func(ctx context.Context) (string, error) {
				startBlock <- struct{}{}
				<-releaseBlock
				return "ok", nil
			})
		}()
	}

	// Wait for both to be actively executing
	<-startBlock
	<-startBlock

	if b.ActiveCount() != 2 {
		t.Fatalf("expected 2 active, got %d", b.ActiveCount())
	}

	// Launch 3rd worker which must enter queue
	queuedDone := make(chan struct{})
	go func() {
		res, err := b.Execute(ctx, func(ctx context.Context) (string, error) {
			return "queued-res", nil
		})
		if err != nil {
			t.Errorf("queued task failed: %v", err)
		}
		if res != "queued-res" {
			t.Errorf("expected queued-res, got %s", res)
		}
		close(queuedDone)
	}()

	// Brief pause to allow entering queue
	time.Sleep(20 * time.Millisecond)
	if b.QueueCount() != 1 {
		t.Errorf("expected 1 queued, got %d", b.QueueCount())
	}

	// Release one active slot
	releaseBlock <- struct{}{}

	select {
	case <-queuedDone:
		// Succeeded!
	case <-time.After(500 * time.Millisecond):
		t.Fatal("queued task did not finish in time")
	}

	// Release remaining active worker
	releaseBlock <- struct{}{}
}

func TestBulkhead_FullRejection(t *testing.T) {
	b := New[int](Config{
		MaxConcurrent: 1,
		MaxWaitQueue:  1,
		QueueTimeout:  100 * time.Millisecond,
	})

	ctx := context.Background()
	release := make(chan struct{})
	started := make(chan struct{})

	// 1st occupies active slot
	go func() {
		_, _ = b.Execute(ctx, func(ctx context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		})
	}()
	<-started

	// 2nd occupies queue slot
	go func() {
		_, _ = b.Execute(ctx, func(ctx context.Context) (int, error) {
			<-release
			return 2, nil
		})
	}()
	time.Sleep(15 * time.Millisecond)

	// 3rd should be rejected immediately because active + queue are saturated
	_, err := b.Execute(ctx, func(ctx context.Context) (int, error) {
		return 3, nil
	})

	if !errors.Is(err, ErrBulkheadFull) {
		t.Fatalf("expected ErrBulkheadFull, got %v", err)
	}

	stats := b.Stats()
	if stats.TotalRejected != 1 {
		t.Errorf("expected 1 rejected, got %d", stats.TotalRejected)
	}

	close(release)
}

func TestBulkhead_QueueTimeout(t *testing.T) {
	b := New[int](Config{
		MaxConcurrent: 1,
		MaxWaitQueue:  2,
		QueueTimeout:  25 * time.Millisecond,
	})

	ctx := context.Background()
	release := make(chan struct{})
	started := make(chan struct{})

	// Block active execution
	go func() {
		_, _ = b.Execute(ctx, func(ctx context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		})
	}()
	<-started

	// This queued request should time out waiting for active slot
	_, err := b.Execute(ctx, func(ctx context.Context) (int, error) {
		return 2, nil
	})

	if !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("expected ErrQueueTimeout, got %v", err)
	}

	stats := b.Stats()
	if stats.TotalTimedOut != 1 {
		t.Errorf("expected 1 timed out, got %d", stats.TotalTimedOut)
	}

	close(release)
}

func TestBulkhead_ContextCancellation(t *testing.T) {
	b := New[int](Config{
		MaxConcurrent: 1,
		MaxWaitQueue:  2,
		QueueTimeout:  500 * time.Millisecond,
	})

	release := make(chan struct{})
	started := make(chan struct{})

	go func() {
		_, _ = b.Execute(context.Background(), func(ctx context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		})
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := b.Execute(ctx, func(ctx context.Context) (int, error) {
		return 2, nil
	})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	close(release)
}

func TestBulkhead_TryExecute(t *testing.T) {
	b := New[int](Config{
		MaxConcurrent: 1,
	})

	release := make(chan struct{})
	started := make(chan struct{})

	go func() {
		_, _ = b.TryExecute(func() (int, error) {
			close(started)
			<-release
			return 100, nil
		})
	}()
	<-started

	// Fast-path rejected
	_, err := b.TryExecute(func() (int, error) {
		return 200, nil
	})
	if !errors.Is(err, ErrBulkheadFull) {
		t.Fatalf("expected ErrBulkheadFull on TryExecute, got %v", err)
	}

	close(release)
	time.Sleep(10 * time.Millisecond)

	// Now should succeed
	res, err := b.TryExecute(func() (int, error) {
		return 300, nil
	})
	if err != nil || res != 300 {
		t.Fatalf("expected 300 with nil error, got %d (%v)", res, err)
	}
}

func TestBulkhead_ConcurrentStress(t *testing.T) {
	const workers = 50
	const concurrent = 5
	b := New[int](Config{
		MaxConcurrent: concurrent,
		MaxWaitQueue:  workers,
		QueueTimeout:  500 * time.Millisecond,
	})

	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, _ = b.Execute(ctx, func(ctx context.Context) (int, error) {
				time.Sleep(2 * time.Millisecond)
				return id, nil
			})
		}(i)
	}

	wg.Wait()

	stats := b.Stats()
	if stats.TotalExecuted+stats.TotalRejected+stats.TotalTimedOut != workers {
		t.Fatalf("expected sum of outcomes to be %d, got stats: %+v", workers, stats)
	}
	if b.ActiveCount() != 0 {
		t.Errorf("expected activeCount 0, got %d", b.ActiveCount())
	}
	if b.QueueCount() != 0 {
		t.Errorf("expected queueCount 0, got %d", b.QueueCount())
	}
}
