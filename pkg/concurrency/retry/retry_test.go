package retry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var (
	errTransient = errors.New("network timeout - temporary")
	errPermanent = errors.New("invalid credentials - permanent")
)

func TestRetry_SuccessImmediate(t *testing.T) {
	ctx := context.Background()
	calls := 0

	res, err := Do(ctx, func(ctx context.Context) (string, error) {
		calls++
		return "success", nil
	}, WithMaxAttempts(3))

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if res != "success" {
		t.Fatalf("expected success, got %s", res)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetry_SuccessAfterTransientErrors(t *testing.T) {
	ctx := context.Background()
	calls := 0
	retriesObserved := 0

	retrier := New[int](
		WithMaxAttempts(4),
		WithInitialInterval(5*time.Millisecond),
		WithMaxInterval(50*time.Millisecond),
		WithJitter(NoJitter),
		WithOnRetry(func(attempt int, err error, nextBackoff time.Duration) {
			retriesObserved++
		}),
	)

	res, err := retrier.Execute(ctx, func(ctx context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, errTransient
		}
		return 100, nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if res != 100 {
		t.Fatalf("expected 100, got %d", res)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if retriesObserved != 2 {
		t.Fatalf("expected 2 retries observed, got %d", retriesObserved)
	}

	stats := retrier.Stats()
	if stats.SuccessfulCalls != 1 {
		t.Errorf("expected 1 successful call, got %d", stats.SuccessfulCalls)
	}
	if stats.TotalRetries != 2 {
		t.Errorf("expected 2 total retries, got %d", stats.TotalRetries)
	}
}

func TestRetry_PermanentErrorAbortsImmediately(t *testing.T) {
	ctx := context.Background()
	calls := 0

	retrier := New[string](
		WithMaxAttempts(5),
		WithInitialInterval(5*time.Millisecond),
		WithRetryIf(func(err error) bool {
			return !errors.Is(err, errPermanent)
		}),
	)

	_, err := retrier.Execute(ctx, func(ctx context.Context) (string, error) {
		calls++
		return "", errPermanent
	})

	if !errors.Is(err, errPermanent) {
		t.Fatalf("expected errPermanent, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected fail-fast on 1st call, but made %d calls", calls)
	}

	stats := retrier.Stats()
	if stats.PermanentFailures != 1 {
		t.Errorf("expected 1 permanent failure, got %d", stats.PermanentFailures)
	}
	if stats.TotalRetries != 0 {
		t.Errorf("expected 0 retries, got %d", stats.TotalRetries)
	}
}

func TestRetry_ExhaustAttempts(t *testing.T) {
	ctx := context.Background()
	calls := 0

	retrier := New[string](
		WithMaxAttempts(3),
		WithInitialInterval(5*time.Millisecond),
		WithJitter(NoJitter),
	)

	_, err := retrier.Execute(ctx, func(ctx context.Context) (string, error) {
		calls++
		return "", errTransient
	})

	if !errors.Is(err, ErrMaxAttemptsReached) {
		t.Fatalf("expected ErrMaxAttemptsReached, got %v", err)
	}
	if !errors.Is(err, errTransient) {
		t.Fatalf("expected unwrapped error to match errTransient, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}

	stats := retrier.Stats()
	if stats.PermanentFailures != 1 {
		t.Errorf("expected 1 failure recorded, got %d", stats.PermanentFailures)
	}
	if stats.TotalRetries != 2 {
		t.Errorf("expected 2 retries recorded, got %d", stats.TotalRetries)
	}
}

func TestRetry_ContextCancellationDuringSleep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	calls := 0
	start := time.Now()

	_, err := Do(ctx, func(ctx context.Context) (string, error) {
		calls++
		return "", errTransient
	},
		WithMaxAttempts(5),
		WithInitialInterval(200*time.Millisecond), // Longer than context deadline
		WithJitter(NoJitter),
	)

	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected only 1 call before timeout, got %d", calls)
	}
	if elapsed > 150*time.Millisecond {
		t.Fatalf("expected fast abort on context timeout, took %v", elapsed)
	}
}

func TestRetry_JitterStrategies(t *testing.T) {
	ctx := context.Background()

	strategies := []JitterStrategy{FullJitter, EqualJitter, NoJitter}

	for _, strat := range strategies {
		var observedBackoff time.Duration
		retrier := New[int](
			WithMaxAttempts(2),
			WithInitialInterval(10*time.Millisecond),
			WithMaxInterval(20*time.Millisecond),
			WithJitter(strat),
			WithOnRetry(func(attempt int, err error, nextBackoff time.Duration) {
				observedBackoff = nextBackoff
			}),
		)

		calls := 0
		_, _ = retrier.Execute(ctx, func(ctx context.Context) (int, error) {
			calls++
			if calls == 1 {
				return 0, errTransient
			}
			return 42, nil
		})

		if strat == NoJitter && observedBackoff != 10*time.Millisecond {
			t.Errorf("NoJitter expected exactly 10ms, got %v", observedBackoff)
		}
		if strat == EqualJitter && (observedBackoff < 5*time.Millisecond || observedBackoff > 10*time.Millisecond) {
			t.Errorf("EqualJitter out of range [5ms, 10ms]: %v", observedBackoff)
		}
		if strat == FullJitter && observedBackoff > 10*time.Millisecond {
			t.Errorf("FullJitter exceeded max base: %v", observedBackoff)
		}
	}
}

func TestRetry_ConcurrentStress(t *testing.T) {
	const workers = 30
	retrier := New[int](
		WithMaxAttempts(3),
		WithInitialInterval(2*time.Millisecond),
		WithMaxInterval(10*time.Millisecond),
		WithJitter(FullJitter),
	)

	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			calls := 0
			_, _ = retrier.Execute(ctx, func(ctx context.Context) (int, error) {
				calls++
				if calls == 1 {
					return 0, errTransient
				}
				return id, nil
			})
		}(i)
	}

	wg.Wait()

	stats := retrier.Stats()
	if stats.SuccessfulCalls != workers {
		t.Fatalf("expected %d successful calls, got %d", workers, stats.SuccessfulCalls)
	}
	if stats.TotalRetries != workers {
		t.Fatalf("expected %d retries, got %d", workers, stats.TotalRetries)
	}
}
