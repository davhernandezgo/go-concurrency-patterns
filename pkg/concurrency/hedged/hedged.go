package hedged

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

var (
	// ErrNoTasks is returned when Execute is called with no task functions.
	ErrNoTasks = errors.New("hedged: at least one task function must be provided")

	// ErrAllFailed is returned when all tasks (primary and hedged) fail with errors.
	ErrAllFailed = errors.New("hedged: all hedged requests failed")
)

// Task represents an executable operation that respects context cancellation.
type Task[T any] func(ctx context.Context) (T, error)

// AggregateError records multiple failed task attempts.
type AggregateError struct {
	Errors []error
}

func (a *AggregateError) Error() string {
	var sb strings.Builder
	sb.WriteString("hedged: all tasks failed: [")
	for i, err := range a.Errors {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(fmt.Sprintf("task %d: %v", i+1, err))
	}
	sb.WriteString("]")
	return sb.String()
}

func (a *AggregateError) Unwrap() []error {
	return a.Errors
}

// StatsSnapshot captures a point-in-time telemetry snapshot of the hedged engine.
type StatsSnapshot struct {
	TotalRequests    uint64
	PrimaryWins      uint64
	HedgeWins        uint64
	HedgesDispatched uint64
	Errors           uint64
}

// WinRatio returns the percentage of requests won by hedged secondary requests.
func (s StatsSnapshot) WinRatio() float64 {
	totalWins := s.PrimaryWins + s.HedgeWins
	if totalWins == 0 {
		return 0.0
	}
	return (float64(s.HedgeWins) / float64(totalWins)) * 100.0
}

// options configures the behavior of a hedged engine.
type options struct {
	delay     time.Duration
	maxHedges int
}

// Option modifies engine options.
type Option func(*options)

// WithDelay sets the delay interval before dispatching each successive hedged request.
// If delay <= 0, all tasks are launched concurrently in parallel (first-success wins).
func WithDelay(d time.Duration) Option {
	return func(o *options) {
		o.delay = d
	}
}

// WithMaxHedges caps the maximum number of hedged (secondary) requests dispatched.
// By default, maxHedges <= 0 allows all provided backup tasks to be hedged.
func WithMaxHedges(n int) Option {
	return func(o *options) {
		o.maxHedges = n
	}
}

// Engine orchestrates speculative hedged requests to combat tail latency in distributed calls.
type Engine[T any] struct {
	opts options

	totalRequests    atomic.Uint64
	primaryWins      atomic.Uint64
	hedgeWins        atomic.Uint64
	hedgesDispatched atomic.Uint64
	errors           atomic.Uint64
}

// New creates a new Hedged Engine with the specified options.
func New[T any](opts ...Option) *Engine[T] {
	cfg := options{
		delay:     50 * time.Millisecond,
		maxHedges: 0,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Engine[T]{
		opts: cfg,
	}
}

// Stats returns an atomic snapshot of engine metrics.
func (e *Engine[T]) Stats() StatsSnapshot {
	return StatsSnapshot{
		TotalRequests:    e.totalRequests.Load(),
		PrimaryWins:      e.primaryWins.Load(),
		HedgeWins:        e.hedgeWins.Load(),
		HedgesDispatched: e.hedgesDispatched.Load(),
		Errors:           e.errors.Load(),
	}
}

type taskResult[T any] struct {
	val     T
	err     error
	idx     int
	isHedge bool
}

// Execute runs the primary task and, if it does not complete within the configured
// delay, speculatively fires subsequent tasks until one succeeds or all fail.
// When any task succeeds, sibling tasks are canceled immediately via context.
func (e *Engine[T]) Execute(ctx context.Context, tasks ...Task[T]) (T, error) {
	var zero T
	n := len(tasks)
	if n == 0 {
		return zero, ErrNoTasks
	}

	e.totalRequests.Add(1)

	// Fast-path: single task has no hedges to dispatch
	if n == 1 {
		val, err := tasks[0](ctx)
		if err != nil {
			e.errors.Add(1)
			return zero, fmt.Errorf("%w: %v", ErrAllFailed, err)
		}
		e.primaryWins.Add(1)
		return val, nil
	}

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Channel buffer sized to total tasks to guarantee zero goroutine leaks on completion
	resCh := make(chan taskResult[T], n)

	var (
		activeWorkers    int
		hedgesDispatched int
		nextTaskIdx      int
		collectedErrors  []error
	)

	launchTask := func(idx int, isHedge bool) {
		activeWorkers++
		if isHedge {
			hedgesDispatched++
			e.hedgesDispatched.Add(1)
		}
		go func() {
			val, err := tasks[idx](childCtx)
			resCh <- taskResult[T]{
				val:     val,
				err:     err,
				idx:     idx,
				isHedge: isHedge,
			}
		}()
	}

	canHedgeMore := func() bool {
		if nextTaskIdx >= n {
			return false
		}
		if e.opts.maxHedges > 0 && hedgesDispatched >= e.opts.maxHedges {
			return false
		}
		return true
	}

	// If delay <= 0, dispatch all tasks concurrently
	if e.opts.delay <= 0 {
		for i := 0; i < n; i++ {
			if i > 0 && e.opts.maxHedges > 0 && hedgesDispatched >= e.opts.maxHedges {
				break
			}
			launchTask(i, i > 0)
			nextTaskIdx = i + 1
		}
	} else {
		// Launch primary task
		launchTask(0, false)
		nextTaskIdx = 1
	}

	var hedgeTimer *time.Timer
	var timerChan <-chan time.Time

	armTimer := func() {
		if e.opts.delay > 0 && canHedgeMore() {
			if hedgeTimer == nil {
				hedgeTimer = time.NewTimer(e.opts.delay)
			} else {
				if !hedgeTimer.Stop() {
					select {
					case <-hedgeTimer.C:
					default:
					}
				}
				hedgeTimer.Reset(e.opts.delay)
			}
			timerChan = hedgeTimer.C
		} else {
			timerChan = nil
		}
	}

	if e.opts.delay > 0 && canHedgeMore() {
		armTimer()
		defer func() {
			if hedgeTimer != nil {
				hedgeTimer.Stop()
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			e.errors.Add(1)
			return zero, ctx.Err()

		case <-timerChan:
			// Hedge delay elapsed; dispatch next speculative task
			if canHedgeMore() {
				launchTask(nextTaskIdx, true)
				nextTaskIdx++
				armTimer()
			} else {
				timerChan = nil
			}

		case res := <-resCh:
			activeWorkers--
			if res.err == nil {
				// Winner found! Cancel all other in-flight tasks and return
				cancel()
				if res.isHedge {
					e.hedgeWins.Add(1)
				} else {
					e.primaryWins.Add(1)
				}
				return res.val, nil
			}

			// Task returned an error
			collectedErrors = append(collectedErrors, res.err)

			// If other tasks haven't been dispatched yet, dispatch immediately
			// rather than waiting for hedge timer to expire
			if canHedgeMore() {
				launchTask(nextTaskIdx, true)
				nextTaskIdx++
				armTimer()
			}

			// If no more active workers and no more tasks can be dispatched, all failed
			if activeWorkers == 0 && !canHedgeMore() {
				e.errors.Add(1)
				return zero, fmt.Errorf("%w: %w", ErrAllFailed, &AggregateError{Errors: collectedErrors})
			}
		}
	}
}

// ExecuteWithDelay is a convenience function that executes hedged tasks with a fixed delay.
func ExecuteWithDelay[T any](ctx context.Context, delay time.Duration, tasks ...Task[T]) (T, error) {
	engine := New[T](WithDelay(delay))
	return engine.Execute(ctx, tasks...)
}

// Execute is a convenience function that executes hedged tasks with default 50ms delay.
func Execute[T any](ctx context.Context, tasks ...Task[T]) (T, error) {
	return ExecuteWithDelay(ctx, 50*time.Millisecond, tasks...)
}
