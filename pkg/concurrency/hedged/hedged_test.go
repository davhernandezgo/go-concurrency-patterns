package hedged

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHedged_PrimaryWinsFast(t *testing.T) {
	engine := New[string](WithDelay(50 * time.Millisecond))
	ctx := context.Background()

	var hedgeCalled atomic.Bool
	primary := func(ctx context.Context) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "primary-success", nil
	}
	hedge := func(ctx context.Context) (string, error) {
		hedgeCalled.Store(true)
		return "hedge-success", nil
	}

	res, err := engine.Execute(ctx, primary, hedge)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "primary-success" {
		t.Fatalf("expected primary-success, got %s", res)
	}
	if hedgeCalled.Load() {
		t.Fatalf("expected hedge task NOT to be called when primary wins before delay")
	}

	stats := engine.Stats()
	if stats.PrimaryWins != 1 || stats.HedgeWins != 0 || stats.HedgesDispatched != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestHedged_HedgeWinsSlowPrimary(t *testing.T) {
	engine := New[string](WithDelay(20 * time.Millisecond))
	ctx := context.Background()

	var primaryCanceled atomic.Bool

	primary := func(ctx context.Context) (string, error) {
		select {
		case <-time.After(150 * time.Millisecond):
			return "slow-primary", nil
		case <-ctx.Done():
			primaryCanceled.Store(true)
			return "", ctx.Err()
		}
	}

	hedge := func(ctx context.Context) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "fast-hedge-replica", nil
	}

	res, err := engine.Execute(ctx, primary, hedge)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "fast-hedge-replica" {
		t.Fatalf("expected fast-hedge-replica, got %s", res)
	}

	// Wait briefly for primary cancel propagation
	time.Sleep(20 * time.Millisecond)
	if !primaryCanceled.Load() {
		t.Fatalf("expected primary task to be canceled upon hedge victory")
	}

	stats := engine.Stats()
	if stats.PrimaryWins != 0 || stats.HedgeWins != 1 || stats.HedgesDispatched != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.WinRatio() != 100.0 {
		t.Fatalf("expected win ratio 100%%, got %f", stats.WinRatio())
	}
}

func TestHedged_PrimaryFailsFastHedgeSucceeds(t *testing.T) {
	// Set long delay to prove that fast failure triggers next hedge immediately without waiting
	engine := New[string](WithDelay(500 * time.Millisecond))
	ctx := context.Background()

	start := time.Now()
	primary := func(ctx context.Context) (string, error) {
		return "", errors.New("connection reset by peer")
	}
	hedge := func(ctx context.Context) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "backup-replica-ok", nil
	}

	res, err := engine.Execute(ctx, primary, hedge)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "backup-replica-ok" {
		t.Fatalf("expected backup-replica-ok, got %s", res)
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("expected fast hedge dispatch on primary error, took %v", elapsed)
	}
}

func TestHedged_AllTasksFail(t *testing.T) {
	engine := New[int](WithDelay(10 * time.Millisecond))
	ctx := context.Background()

	err1 := errors.New("database unreachable")
	err2 := errors.New("read replica timeout")

	t1 := func(ctx context.Context) (int, error) {
		return 0, err1
	}
	t2 := func(ctx context.Context) (int, error) {
		return 0, err2
	}

	val, err := engine.Execute(ctx, t1, t2)
	if val != 0 {
		t.Fatalf("expected 0 value, got %d", val)
	}
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrAllFailed) {
		t.Fatalf("expected ErrAllFailed in error chain, got %v", err)
	}

	var aggErr *AggregateError
	if !errors.As(err, &aggErr) {
		t.Fatalf("expected AggregateError, got %T: %v", err, err)
	}
	if len(aggErr.Errors) != 2 {
		t.Fatalf("expected 2 aggregated errors, got %d", len(aggErr.Errors))
	}

	stats := engine.Stats()
	if stats.Errors != 1 {
		t.Fatalf("expected 1 error count, got %d", stats.Errors)
	}
}

func TestHedged_ContextCancellation(t *testing.T) {
	engine := New[string](WithDelay(20 * time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	var startedTasks atomic.Int32
	hangingTask := func(ctx context.Context) (string, error) {
		startedTasks.Add(1)
		<-ctx.Done()
		return "", ctx.Err()
	}

	_, err := engine.Execute(ctx, hangingTask, hangingTask, hangingTask)
	if err == nil {
		t.Fatalf("expected context timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestHedged_ParallelScatterGather(t *testing.T) {
	// delay <= 0 triggers immediate parallel execution
	engine := New[int](WithDelay(0))
	ctx := context.Background()

	slowTask := func(ctx context.Context) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 100, nil
	}
	fastTask := func(ctx context.Context) (int, error) {
		time.Sleep(10 * time.Millisecond)
		return 42, nil
	}

	res, err := engine.Execute(ctx, slowTask, fastTask)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 42 {
		t.Fatalf("expected fastest result 42, got %d", res)
	}
}

func TestHedged_ZeroTasksValidation(t *testing.T) {
	engine := New[string]()
	_, err := engine.Execute(context.Background())
	if !errors.Is(err, ErrNoTasks) {
		t.Fatalf("expected ErrNoTasks, got %v", err)
	}
}

func TestHedged_SingleTask(t *testing.T) {
	engine := New[string]()
	res, err := engine.Execute(context.Background(), func(ctx context.Context) (string, error) {
		return "solo", nil
	})
	if err != nil || res != "solo" {
		t.Fatalf("unexpected result: val=%s, err=%v", res, err)
	}

	stats := engine.Stats()
	if stats.PrimaryWins != 1 {
		t.Fatalf("expected primary win for single task, got %+v", stats)
	}
}

func TestHedged_MaxHedges(t *testing.T) {
	// Allow only 1 hedge even though 4 tasks are provided
	engine := New[string](WithDelay(10*time.Millisecond), WithMaxHedges(1))
	ctx := context.Background()

	var dispatched atomic.Int32
	slowFail := func(ctx context.Context) (string, error) {
		dispatched.Add(1)
		time.Sleep(50 * time.Millisecond)
		return "", errors.New("failed")
	}

	_, err := engine.Execute(ctx, slowFail, slowFail, slowFail, slowFail)
	if !errors.Is(err, ErrAllFailed) {
		t.Fatalf("expected ErrAllFailed, got %v", err)
	}

	// Primary (1) + MaxHedges (1) = 2 total tasks dispatched
	if count := dispatched.Load(); count != 2 {
		t.Fatalf("expected exactly 2 dispatched tasks with MaxHedges=1, got %d", count)
	}
}

func TestHedged_ConcurrentStressRace(t *testing.T) {
	engine := New[string](WithDelay(5 * time.Millisecond))
	ctx := context.Background()

	const concurrency = 50
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()

			t1 := func(ctx context.Context) (string, error) {
				if id%2 == 0 {
					return fmt.Sprintf("res-%d", id), nil
				}
				time.Sleep(20 * time.Millisecond)
				return fmt.Sprintf("slow-%d", id), nil
			}

			t2 := func(ctx context.Context) (string, error) {
				time.Sleep(8 * time.Millisecond)
				return fmt.Sprintf("res-%d", id), nil
			}

			val, err := engine.Execute(ctx, t1, t2)
			if err != nil {
				t.Errorf("task %d failed: %v", id, err)
			}
			expected := fmt.Sprintf("res-%d", id)
			if val != expected {
				t.Errorf("got %s, expected %s", val, expected)
			}
		}(i)
	}

	wg.Wait()

	stats := engine.Stats()
	if stats.TotalRequests != concurrency {
		t.Fatalf("expected %d total requests, got %d", concurrency, stats.TotalRequests)
	}
	if stats.PrimaryWins+stats.HedgeWins != concurrency {
		t.Fatalf("all requests should have won (primary or hedge), stats=%+v", stats)
	}
}
