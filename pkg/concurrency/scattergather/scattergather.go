package scattergather

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
	ErrNoTasks = errors.New("scattergather: at least one task function must be provided")

	// ErrQuorumNotMet is returned when fewer successful results than the required quorum were obtained.
	ErrQuorumNotMet = errors.New("scattergather: required quorum of successful tasks was not achieved")
)

// Task represents an executable operation that receives an operation context and returns a typed result.
type Task[T any] func(ctx context.Context) (T, error)

// TaskResult encapsulates the individual execution outcome of a scattered task.
type TaskResult[T any] struct {
	Index    int
	Value    T
	Err      error
	Duration time.Duration
}

// Aggregator folds successful and collected results into a single consolidated structure.
type Aggregator[T any, R any] func(results []TaskResult[T]) (R, error)

// AggregateError records multiple failed task attempts when quorum cannot be met.
type AggregateError struct {
	Errors []error
}

func (a *AggregateError) Error() string {
	var sb strings.Builder
	sb.WriteString("scattergather: quorum failed: [")
	for i, err := range a.Errors {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(fmt.Sprintf("%v", err))
	}
	sb.WriteString("]")
	return sb.String()
}

func (a *AggregateError) Unwrap() []error {
	return a.Errors
}

// Config defines the execution and resilience policy for a Scatter-Gather operation.
type Config struct {
	// MinQuorum specifies how many successful tasks are required to satisfy the request.
	// If MinQuorum <= 0 or > len(tasks), all tasks must succeed.
	MinQuorum int

	// Timeout specifies the maximum duration allowed for the entire scatter-gather lifecycle.
	// If <= 0, execution is bound only by the caller's context.
	Timeout time.Duration

	// MaxConcurrency bounds the maximum number of worker goroutines executing concurrently.
	// If <= 0, all tasks execute simultaneously.
	MaxConcurrency int

	// FastExitOnQuorum when true immediately cancels slower in-flight tasks and returns
	// as soon as MinQuorum successful responses are collected. Defaults to true.
	FastExitOnQuorum bool

	// AllowPartialOnTimeout when true returns partial results if the timeout expires but
	// at least MinQuorum successful results were collected. Defaults to true.
	AllowPartialOnTimeout bool
}

// Option configures the ScatterGather Engine.
type Option[T any] func(*Engine[T])

// WithMinQuorum sets the minimum number of successful tasks required.
func WithMinQuorum[T any](quorum int) Option[T] {
	return func(e *Engine[T]) {
		e.cfg.MinQuorum = quorum
	}
}

// WithTimeout sets a maximum gathering deadline.
func WithTimeout[T any](timeout time.Duration) Option[T] {
	return func(e *Engine[T]) {
		e.cfg.Timeout = timeout
	}
}

// WithMaxConcurrency limits concurrent execution to prevent resource exhaustion.
func WithMaxConcurrency[T any](n int) Option[T] {
	return func(e *Engine[T]) {
		e.cfg.MaxConcurrency = n
	}
}

// WithFastExitOnQuorum controls whether the engine immediately finishes once quorum is met.
func WithFastExitOnQuorum[T any](fastExit bool) Option[T] {
	return func(e *Engine[T]) {
		e.cfg.FastExitOnQuorum = fastExit
	}
}

// WithAllowPartialOnTimeout controls whether partial successes (>= MinQuorum) are accepted on timeout.
func WithAllowPartialOnTimeout[T any](allow bool) Option[T] {
	return func(e *Engine[T]) {
		e.cfg.AllowPartialOnTimeout = allow
	}
}

// StatsSnapshot captures telemetry metrics for the ScatterGather engine.
type StatsSnapshot struct {
	TotalOperations  uint64
	QuorumEarlyExits uint64
	FullCompletions  uint64
	TimeoutDegraded  uint64
	QuorumFailures   uint64
	TasksDispatched  uint64
	TasksCancelled   uint64
}

// QuorumEarlyExitRatio returns the percentage of successful operations that exited early due to quorum.
func (s StatsSnapshot) QuorumEarlyExitRatio() float64 {
	completed := s.QuorumEarlyExits + s.FullCompletions
	if completed == 0 {
		return 0.0
	}
	return (float64(s.QuorumEarlyExits) / float64(completed)) * 100.0
}

// SuccessRatio returns the percentage of total operations that satisfied quorum.
func (s StatsSnapshot) SuccessRatio() float64 {
	if s.TotalOperations == 0 {
		return 0.0
	}
	successful := s.QuorumEarlyExits + s.FullCompletions + s.TimeoutDegraded
	return (float64(successful) / float64(s.TotalOperations)) * 100.0
}

// Engine coordinates scatter-gather operations across concurrent tasks with quorum and cancellation semantics.
type Engine[T any] struct {
	cfg Config

	totalOperations  atomic.Uint64
	quorumEarlyExits atomic.Uint64
	fullCompletions  atomic.Uint64
	timeoutDegraded  atomic.Uint64
	quorumFailures   atomic.Uint64
	tasksDispatched  atomic.Uint64
	tasksCancelled   atomic.Uint64
}

// New creates a new Generic ScatterGather Engine configured with functional options.
func New[T any](opts ...Option[T]) *Engine[T] {
	e := &Engine[T]{
		cfg: Config{
			MinQuorum:             0,
			Timeout:               0,
			MaxConcurrency:        0,
			FastExitOnQuorum:      true,
			AllowPartialOnTimeout: true,
		},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Stats returns an atomic snapshot of execution telemetry.
func (e *Engine[T]) Stats() StatsSnapshot {
	return StatsSnapshot{
		TotalOperations:  e.totalOperations.Load(),
		QuorumEarlyExits: e.quorumEarlyExits.Load(),
		FullCompletions:  e.fullCompletions.Load(),
		TimeoutDegraded:  e.timeoutDegraded.Load(),
		QuorumFailures:   e.quorumFailures.Load(),
		TasksDispatched:  e.tasksDispatched.Load(),
		TasksCancelled:   e.tasksCancelled.Load(),
	}
}

// Execute broadcasts the given tasks concurrently, gathers their results, and enforces quorum.
// If FastExitOnQuorum is enabled (default), it cancels all remaining slow tasks as soon as MinQuorum
// successful completions arrive, significantly truncating high-percentile latency.
func (e *Engine[T]) Execute(ctx context.Context, tasks ...Task[T]) ([]TaskResult[T], error) {
	if len(tasks) == 0 {
		return nil, ErrNoTasks
	}

	e.totalOperations.Add(1)

	quorum := e.cfg.MinQuorum
	if quorum <= 0 || quorum > len(tasks) {
		quorum = len(tasks)
	}

	var opCtx context.Context
	var cancel context.CancelFunc
	if e.cfg.Timeout > 0 {
		opCtx, cancel = context.WithTimeout(ctx, e.cfg.Timeout)
	} else {
		opCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	totalTasks := len(tasks)
	resultsChan := make(chan TaskResult[T], totalTasks)

	var sem chan struct{}
	if e.cfg.MaxConcurrency > 0 {
		sem = make(chan struct{}, e.cfg.MaxConcurrency)
	}

	for i, t := range tasks {
		e.tasksDispatched.Add(1)
		go func(idx int, fn Task[T]) {
			if sem != nil {
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-opCtx.Done():
					e.tasksCancelled.Add(1)
					resultsChan <- TaskResult[T]{
						Index: idx,
						Err:   opCtx.Err(),
					}
					return
				}
			}

			if err := opCtx.Err(); err != nil {
				e.tasksCancelled.Add(1)
				resultsChan <- TaskResult[T]{
					Index: idx,
					Err:   err,
				}
				return
			}

			start := time.Now()
			val, err := fn(opCtx)
			duration := time.Since(start)

			if err != nil && errors.Is(opCtx.Err(), context.Canceled) {
				e.tasksCancelled.Add(1)
			}

			resultsChan <- TaskResult[T]{
				Index:    idx,
				Value:    val,
				Err:      err,
				Duration: duration,
			}
		}(i, t)
	}

	results := make([]TaskResult[T], 0, totalTasks)
	var taskErrors []error
	successCount := 0
	remaining := totalTasks

	for remaining > 0 {
		select {
		case res := <-resultsChan:
			results = append(results, res)
			remaining--

			if res.Err == nil {
				successCount++
				if e.cfg.FastExitOnQuorum && successCount >= quorum {
					cancel() // cancel pending slower tasks early
					if successCount == totalTasks {
						e.fullCompletions.Add(1)
					} else {
						e.quorumEarlyExits.Add(1)
					}
					return results, nil
				}
			} else {
				taskErrors = append(taskErrors, res.Err)
				// Mathematical fail-fast check: if even all remaining tasks succeed,
				// can we still meet the required quorum?
				if successCount+remaining < quorum {
					cancel()
					e.quorumFailures.Add(1)
					return results, &AggregateError{Errors: append([]error{ErrQuorumNotMet}, taskErrors...)}
				}
			}

			if remaining == 0 {
				if successCount >= quorum {
					e.fullCompletions.Add(1)
					return results, nil
				}
				e.quorumFailures.Add(1)
				return results, &AggregateError{Errors: append([]error{ErrQuorumNotMet}, taskErrors...)}
			}

		case <-opCtx.Done():
			// Non-blockingly drain whatever has already arrived in resultsChan
		drainLoop:
			for {
				select {
				case res := <-resultsChan:
					results = append(results, res)
					if res.Err == nil {
						successCount++
					} else {
						taskErrors = append(taskErrors, res.Err)
					}
				default:
					break drainLoop
				}
			}

			if successCount >= quorum && e.cfg.AllowPartialOnTimeout && errors.Is(opCtx.Err(), context.DeadlineExceeded) {
				e.timeoutDegraded.Add(1)
				return results, nil
			}

			if errors.Is(opCtx.Err(), context.DeadlineExceeded) {
				e.timeoutDegraded.Add(1)
				return results, fmt.Errorf("%w: timeout exceeded with %d/%d successes", ErrQuorumNotMet, successCount, quorum)
			}

			return results, opCtx.Err()
		}
	}

	if successCount >= quorum {
		e.fullCompletions.Add(1)
		return results, nil
	}

	e.quorumFailures.Add(1)
	return results, &AggregateError{Errors: append([]error{ErrQuorumNotMet}, taskErrors...)}
}

// ExecuteWithAggregator executes tasks and applies a folding aggregator function over the gathered results.
func ExecuteWithAggregator[T any, R any](
	e *Engine[T],
	ctx context.Context,
	aggregator Aggregator[T, R],
	tasks ...Task[T],
) (R, error) {
	var zero R
	results, err := e.Execute(ctx, tasks...)
	if err != nil {
		return zero, err
	}
	return aggregator(results)
}

// Scatter executes tasks concurrently using a default Engine with the specified quorum.
func Scatter[T any](ctx context.Context, minQuorum int, tasks ...Task[T]) ([]TaskResult[T], error) {
	return New[T](WithMinQuorum[T](minQuorum)).Execute(ctx, tasks...)
}

// Gather executes tasks concurrently and aggregates results using aggregator.
func Gather[T any, R any](
	ctx context.Context,
	minQuorum int,
	aggregator Aggregator[T, R],
	tasks ...Task[T],
) (R, error) {
	engine := New[T](WithMinQuorum[T](minQuorum))
	return ExecuteWithAggregator(engine, ctx, aggregator, tasks...)
}
