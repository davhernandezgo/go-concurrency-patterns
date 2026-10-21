package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool_Basic(t *testing.T) {
	var counter int64
	handler := func(ctx context.Context, n int) (int, error) {
		atomic.AddInt64(&counter, 1)
		return n * 2, nil
	}

	pool := New[int, int](4, 10, handler)
	pool.Start()

	totalJobs := 50
	go func() {
		for i := 1; i <= totalJobs; i++ {
			err := pool.Submit(context.Background(), i)
			if err != nil {
				t.Errorf("unexpected error on submit: %v", err)
			}
		}
		pool.Stop()
	}()

	resultsCount := 0
	sumResults := 0
	for res := range pool.Results() {
		if res.Err != nil {
			t.Fatalf("unexpected task error: %v", res.Err)
		}
		sumResults += res.Value
		resultsCount++
	}

	if resultsCount != totalJobs {
		t.Fatalf("expected %d results, got %d", totalJobs, resultsCount)
	}
	if atomic.LoadInt64(&counter) != int64(totalJobs) {
		t.Fatalf("expected handler called %d times, got %d", totalJobs, counter)
	}

	expectedSum := totalJobs * (totalJobs + 1) // sum of 2*i for i=1..50 = 2 * (50*51/2) = 2550
	if sumResults != expectedSum {
		t.Fatalf("expected sum %d, got %d", expectedSum, sumResults)
	}
}

func TestWorkerPool_ErrorHandling(t *testing.T) {
	errBoom := errors.New("boom")
	handler := func(ctx context.Context, item string) (string, error) {
		if item == "fail" {
			return "", errBoom
		}
		return "ok:" + item, nil
	}

	pool := New[string, string](2, 5, handler)
	pool.Start()

	go func() {
		_ = pool.Submit(context.Background(), "item1")
		_ = pool.Submit(context.Background(), "fail")
		_ = pool.Submit(context.Background(), "item2")
		pool.Stop()
	}()

	var errorsFound int
	var successFound int
	for res := range pool.Results() {
		if res.Err != nil {
			errorsFound++
		} else {
			successFound++
		}
	}

	if errorsFound != 1 || successFound != 2 {
		t.Fatalf("expected 1 error and 2 successes, got err=%d, ok=%d", errorsFound, successFound)
	}
}

func TestWorkerPool_TrySubmit(t *testing.T) {
	// 1 worker, 1 queue size
	blockCh := make(chan struct{})
	handler := func(ctx context.Context, val int) (int, error) {
		<-blockCh
		return val, nil
	}

	pool := New[int, int](1, 1, handler)
	pool.Start()

	// Fill worker and queue
	_ = pool.TrySubmit(1) // handled by worker
	time.Sleep(10 * time.Millisecond)
	err1 := pool.TrySubmit(2) // goes into queue
	err2 := pool.TrySubmit(3) // should fail with ErrPoolFull

	if err1 != nil {
		t.Fatalf("expected item 2 to be accepted in queue, got %v", err1)
	}
	if !errors.Is(err2, ErrPoolFull) {
		t.Fatalf("expected ErrPoolFull, got %v", err2)
	}

	go func() {
		for range pool.Results() {
		}
	}()

	close(blockCh)
	pool.Stop()
}
