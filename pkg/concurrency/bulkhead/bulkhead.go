package bulkhead

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var (
	// ErrBulkheadFull is returned when execution capacity and queue limit are both saturated.
	ErrBulkheadFull = errors.New("bulkhead: capacity and wait queue are fully saturated")

	// ErrQueueTimeout is returned when a queued task exceeds its maximum allowed queue wait time.
	ErrQueueTimeout = errors.New("bulkhead: timed out waiting for an execution permit in queue")
)

// Config specifies the concurrency limits and queue buffering for a Bulkhead.
type Config struct {
	// MaxConcurrent is the maximum number of concurrent operations allowed to execute simultaneously.
	// Defaults to 10 if <= 0.
	MaxConcurrent int

	// MaxWaitQueue is the maximum number of requests allowed to wait in the queue for an execution slot.
	// If 0, queueing is disabled (fail-fast rejection).
	MaxWaitQueue int

	// QueueTimeout is the maximum duration a request may remain waiting in queue before timing out.
	// If <= 0, requests wait indefinitely until context cancellation or permit availability.
	QueueTimeout time.Duration
}

// StatsSnapshot captures telemetry metrics for the Bulkhead.
type StatsSnapshot struct {
	ActiveExecutions uint64
	QueuedRequests   uint64
	TotalExecuted    uint64
	TotalRejected    uint64
	TotalTimedOut    uint64
	TotalErrors      uint64
}

// Bulkhead isolates system resources and caps concurrent executions to protect
// critical services from downstream cascading failures.
type Bulkhead[T any] struct {
	cfg Config

	semaphore chan struct{}
	queueSem  chan struct{}

	activeCount atomic.Int64
	queuedCount atomic.Int64

	totalExecuted atomic.Uint64
	totalRejected atomic.Uint64
	totalTimedOut atomic.Uint64
	totalErrors   atomic.Uint64
}

// New creates a new Generic Bulkhead instance initialized with the given Config.
func New[T any](cfg Config) *Bulkhead[T] {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 10
	}
	if cfg.MaxWaitQueue < 0 {
		cfg.MaxWaitQueue = 0
	}

	b := &Bulkhead[T]{
		cfg:       cfg,
		semaphore: make(chan struct{}, cfg.MaxConcurrent),
	}

	if cfg.MaxWaitQueue > 0 {
		b.queueSem = make(chan struct{}, cfg.MaxWaitQueue)
	}

	return b
}

// Stats returns an atomic snapshot of the Bulkhead telemetry.
func (b *Bulkhead[T]) Stats() StatsSnapshot {
	return StatsSnapshot{
		ActiveExecutions: uint64(max(0, b.activeCount.Load())),
		QueuedRequests:   uint64(max(0, b.queuedCount.Load())),
		TotalExecuted:    b.totalExecuted.Load(),
		TotalRejected:    b.totalRejected.Load(),
		TotalTimedOut:    b.totalTimedOut.Load(),
		TotalErrors:      b.totalErrors.Load(),
	}
}

// ActiveCount returns current active executions.
func (b *Bulkhead[T]) ActiveCount() int {
	return int(max(0, b.activeCount.Load()))
}

// QueueCount returns current queued requests.
func (b *Bulkhead[T]) QueueCount() int {
	return int(max(0, b.queuedCount.Load()))
}

// Execute executes fn inside the Bulkhead. If concurrency capacity is saturated,
// the call enters the wait queue if capacity is available. If the queue is also full,
// ErrBulkheadFull is returned immediately.
func (b *Bulkhead[T]) Execute(ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T

	if err := ctx.Err(); err != nil {
		b.totalErrors.Add(1)
		return zero, err
	}

	// 1. Fast-path: try acquiring execution permit immediately without queueing
	select {
	case b.semaphore <- struct{}{}:
		return b.runTask(ctx, fn)
	default:
	}

	// 2. Concurrency saturated: check if queueing is enabled
	if b.queueSem == nil {
		b.totalRejected.Add(1)
		return zero, ErrBulkheadFull
	}

	// 3. Try to enter the wait queue
	select {
	case b.queueSem <- struct{}{}:
		b.queuedCount.Add(1)
		defer func() {
			<-b.queueSem
			b.queuedCount.Add(-1)
		}()
	default:
		// Wait queue is full
		b.totalRejected.Add(1)
		return zero, ErrBulkheadFull
	}

	// 4. Waiting in queue for an execution permit
	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if b.cfg.QueueTimeout > 0 {
		timer = time.NewTimer(b.cfg.QueueTimeout)
		timeoutCh = timer.C
		defer func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()
	}

	select {
	case <-ctx.Done():
		b.totalErrors.Add(1)
		return zero, ctx.Err()

	case <-timeoutCh:
		b.totalTimedOut.Add(1)
		return zero, ErrQueueTimeout

	case b.semaphore <- struct{}{}:
		return b.runTask(ctx, fn)
	}
}

// TryExecute executes fn only if execution capacity is currently available.
// It never waits in queue and returns ErrBulkheadFull if saturated.
func (b *Bulkhead[T]) TryExecute(fn func() (T, error)) (T, error) {
	var zero T

	select {
	case b.semaphore <- struct{}{}:
		b.activeCount.Add(1)
		defer func() {
			<-b.semaphore
			b.activeCount.Add(-1)
		}()

		val, err := fn()
		if err != nil {
			b.totalErrors.Add(1)
			return zero, err
		}
		b.totalExecuted.Add(1)
		return val, nil

	default:
		b.totalRejected.Add(1)
		return zero, ErrBulkheadFull
	}
}

func (b *Bulkhead[T]) runTask(ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T

	b.activeCount.Add(1)
	defer func() {
		<-b.semaphore
		b.activeCount.Add(-1)
	}()

	val, err := fn(ctx)
	if err != nil {
		b.totalErrors.Add(1)
		return zero, err
	}

	b.totalExecuted.Add(1)
	return val, nil
}
