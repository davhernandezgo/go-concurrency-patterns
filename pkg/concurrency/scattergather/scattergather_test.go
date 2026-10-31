package scattergather

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScatterGather_AllTasksSuccess(t *testing.T) {
	engine := New[string](WithMinQuorum[string](3))
	ctx := context.Background()

	tasks := []Task[string]{
		func(ctx context.Context) (string, error) {
			time.Sleep(5 * time.Millisecond)
			return "service-auth-ok", nil
		},
		func(ctx context.Context) (string, error) {
			time.Sleep(10 * time.Millisecond)
			return "service-billing-ok", nil
		},
		func(ctx context.Context) (string, error) {
			time.Sleep(15 * time.Millisecond)
			return "service-inventory-ok", nil
		},
	}

	results, err := engine.Execute(ctx, tasks...)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got: %d", len(results))
	}

	for _, r := range results {
		if r.Err != nil {
			t.Errorf("task %d failed unexpectedly: %v", r.Index, r.Err)
		}
	}

	stats := engine.Stats()
	if stats.TotalOperations != 1 {
		t.Errorf("expected TotalOperations=1, got: %d", stats.TotalOperations)
	}
	if stats.FullCompletions != 1 {
		t.Errorf("expected FullCompletions=1, got: %d", stats.FullCompletions)
	}
}

func TestScatterGather_FastExitOnQuorum(t *testing.T) {
	engine := New[int](
		WithMinQuorum[int](2),
		WithFastExitOnQuorum[int](true),
	)
	ctx := context.Background()

	var slowCancelled atomic.Int32

	tasks := []Task[int]{
		func(ctx context.Context) (int, error) {
			time.Sleep(5 * time.Millisecond)
			return 100, nil
		},
		func(ctx context.Context) (int, error) {
			time.Sleep(8 * time.Millisecond)
			return 200, nil
		},
		func(ctx context.Context) (int, error) {
			select {
			case <-time.After(300 * time.Millisecond):
				return 300, nil
			case <-ctx.Done():
				slowCancelled.Add(1)
				return 0, ctx.Err()
			}
		},
		func(ctx context.Context) (int, error) {
			select {
			case <-time.After(300 * time.Millisecond):
				return 400, nil
			case <-ctx.Done():
				slowCancelled.Add(1)
				return 0, ctx.Err()
			}
		},
	}

	start := time.Now()
	results, err := engine.Execute(ctx, tasks...)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if elapsed > 100*time.Millisecond {
		t.Fatalf("fast exit on quorum took too long: %v (expected < 100ms)", elapsed)
	}

	successCount := 0
	for _, r := range results {
		if r.Err == nil {
			successCount++
		}
	}
	if successCount < 2 {
		t.Fatalf("expected at least 2 successes, got: %d", successCount)
	}

	time.Sleep(30 * time.Millisecond)
	if slowCancelled.Load() == 0 {
		t.Errorf("expected slower in-flight tasks to be cancelled")
	}

	stats := engine.Stats()
	if stats.QuorumEarlyExits != 1 {
		t.Errorf("expected QuorumEarlyExits=1, got: %d", stats.QuorumEarlyExits)
	}
	if ratio := stats.QuorumEarlyExitRatio(); ratio != 100.0 {
		t.Errorf("expected QuorumEarlyExitRatio=100.0%%, got: %.1f%%", ratio)
	}
}

func TestScatterGather_ImpossibleQuorumEarlyAbort(t *testing.T) {
	engine := New[string](WithMinQuorum[string](3))
	ctx := context.Background()

	var slowCancelled atomic.Bool

	tasks := []Task[string]{
		func(ctx context.Context) (string, error) {
			return "", errors.New("cluster-a network partition")
		},
		func(ctx context.Context) (string, error) {
			return "", errors.New("cluster-b timeout")
		},
		func(ctx context.Context) (string, error) {
			select {
			case <-time.After(400 * time.Millisecond):
				return "ok", nil
			case <-ctx.Done():
				slowCancelled.Store(true)
				return "", ctx.Err()
			}
		},
	}

	start := time.Now()
	_, err := engine.Execute(ctx, tasks...)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected error due to impossible quorum, got nil")
	}
	if !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("expected error wrapping ErrQuorumNotMet, got: %v", err)
	}

	if elapsed > 100*time.Millisecond {
		t.Fatalf("fail-fast aborted too slowly: %v (expected < 100ms)", elapsed)
	}

	time.Sleep(20 * time.Millisecond)
	if !slowCancelled.Load() {
		t.Errorf("expected slow task to be cancelled on impossible quorum abort")
	}

	stats := engine.Stats()
	if stats.QuorumFailures != 1 {
		t.Errorf("expected QuorumFailures=1, got: %d", stats.QuorumFailures)
	}
}

func TestScatterGather_TimeoutGracefulDegradation(t *testing.T) {
	engine := New[string](
		WithMinQuorum[string](2),
		WithTimeout[string](40*time.Millisecond),
		WithFastExitOnQuorum[string](false),
		WithAllowPartialOnTimeout[string](true),
	)
	ctx := context.Background()

	tasks := []Task[string]{
		func(ctx context.Context) (string, error) {
			time.Sleep(5 * time.Millisecond)
			return "shard-1", nil
		},
		func(ctx context.Context) (string, error) {
			time.Sleep(10 * time.Millisecond)
			return "shard-2", nil
		},
		func(ctx context.Context) (string, error) {
			select {
			case <-time.After(300 * time.Millisecond):
				return "shard-3", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
	}

	results, err := engine.Execute(ctx, tasks...)
	if err != nil {
		t.Fatalf("expected graceful degradation to succeed with quorum, got: %v", err)
	}

	successCount := 0
	for _, r := range results {
		if r.Err == nil {
			successCount++
		}
	}
	if successCount < 2 {
		t.Fatalf("expected at least 2 partial successes, got: %d", successCount)
	}

	stats := engine.Stats()
	if stats.TimeoutDegraded != 1 {
		t.Errorf("expected TimeoutDegraded=1, got: %d", stats.TimeoutDegraded)
	}
}

func TestScatterGather_TimeoutBelowQuorumFails(t *testing.T) {
	engine := New[string](
		WithMinQuorum[string](3),
		WithTimeout[string](30*time.Millisecond),
		WithFastExitOnQuorum[string](false),
		WithAllowPartialOnTimeout[string](true),
	)
	ctx := context.Background()

	tasks := []Task[string]{
		func(ctx context.Context) (string, error) {
			time.Sleep(5 * time.Millisecond)
			return "replica-1", nil
		},
		func(ctx context.Context) (string, error) {
			select {
			case <-time.After(200 * time.Millisecond):
				return "replica-2", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
		func(ctx context.Context) (string, error) {
			select {
			case <-time.After(200 * time.Millisecond):
				return "replica-3", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
	}

	_, err := engine.Execute(ctx, tasks...)
	if err == nil {
		t.Fatalf("expected timeout below quorum to fail, got nil")
	}
	if !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("expected ErrQuorumNotMet, got: %v", err)
	}
}

func TestScatterGather_ContextCancellation(t *testing.T) {
	engine := New[int](WithMinQuorum[int](2))
	ctx, cancel := context.WithCancel(context.Background())

	tasks := []Task[int]{
		func(c context.Context) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return 1, nil
		},
		func(c context.Context) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return 2, nil
		},
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := engine.Execute(ctx, tasks...)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got: %v", err)
	}
}

func TestScatterGather_MaxConcurrencyLimit(t *testing.T) {
	maxConcurrency := 2
	engine := New[int](
		WithMaxConcurrency[int](maxConcurrency),
		WithMinQuorum[int](4),
	)
	ctx := context.Background()

	var activeCount atomic.Int32
	var peakConcurrency atomic.Int32

	taskCount := 4
	tasks := make([]Task[int], taskCount)
	for i := 0; i < taskCount; i++ {
		tasks[i] = func(ctx context.Context) (int, error) {
			current := activeCount.Add(1)
			defer activeCount.Add(-1)

			for {
				peak := peakConcurrency.Load()
				if current <= peak || peakConcurrency.CompareAndSwap(peak, current) {
					break
				}
			}

			time.Sleep(15 * time.Millisecond)
			return 42, nil
		}
	}

	results, err := engine.Execute(ctx, tasks...)
	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if len(results) != taskCount {
		t.Fatalf("expected %d results, got: %d", taskCount, len(results))
	}

	if peak := int(peakConcurrency.Load()); peak > maxConcurrency {
		t.Errorf("peak concurrency %d exceeded limit of %d", peak, maxConcurrency)
	}
}

func TestScatterGather_AggregatorAndGather(t *testing.T) {
	ctx := context.Background()

	tasks := []Task[int]{
		func(ctx context.Context) (int, error) { return 10, nil },
		func(ctx context.Context) (int, error) { return 20, nil },
		func(ctx context.Context) (int, error) { return 30, nil },
	}

	sumAggregator := func(results []TaskResult[int]) (int, error) {
		total := 0
		for _, r := range results {
			if r.Err == nil {
				total += r.Value
			}
		}
		return total, nil
	}

	total, err := Gather(ctx, 3, sumAggregator, tasks...)
	if err != nil {
		t.Fatalf("unexpected error in Gather: %v", err)
	}
	if total != 60 {
		t.Fatalf("expected aggregated sum 60, got: %d", total)
	}
}

func TestScatterGather_Validation(t *testing.T) {
	engine := New[string]()
	ctx := context.Background()

	_, err := engine.Execute(ctx)
	if !errors.Is(err, ErrNoTasks) {
		t.Fatalf("expected ErrNoTasks when passing 0 tasks, got: %v", err)
	}

	scatterResults, err := Scatter[string](ctx, 1)
	if !errors.Is(err, ErrNoTasks) {
		t.Fatalf("expected ErrNoTasks from Scatter, got: %v", err)
	}
	if scatterResults != nil {
		t.Fatalf("expected nil results on validation error")
	}
}

func TestScatterGather_ConcurrentStressRace(t *testing.T) {
	engine := New[int](
		WithMinQuorum[int](3),
		WithMaxConcurrency[int](5),
		WithFastExitOnQuorum[int](true),
	)

	var wg sync.WaitGroup
	workers := 40

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			tasks := []Task[int]{
				func(ctx context.Context) (int, error) {
					time.Sleep(time.Duration(workerID%3) * time.Millisecond)
					return workerID * 1, nil
				},
				func(ctx context.Context) (int, error) {
					time.Sleep(time.Duration(workerID%4) * time.Millisecond)
					return workerID * 2, nil
				},
				func(ctx context.Context) (int, error) {
					time.Sleep(time.Duration(workerID%5) * time.Millisecond)
					return workerID * 3, nil
				},
				func(ctx context.Context) (int, error) {
					time.Sleep(50 * time.Millisecond)
					return workerID * 4, nil
				},
			}

			_, _ = engine.Execute(ctx, tasks...)
		}(i)
	}

	wg.Wait()

	stats := engine.Stats()
	if stats.TotalOperations != uint64(workers) {
		t.Errorf("expected TotalOperations=%d, got: %d", workers, stats.TotalOperations)
	}
	if ratio := stats.SuccessRatio(); ratio <= 0.0 {
		t.Errorf("expected positive SuccessRatio, got: %.1f%%", ratio)
	}
}
