package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

var (
	// ErrMaxAttemptsReached is returned when the operation exceeds the maximum allowed attempts.
	ErrMaxAttemptsReached = errors.New("retry: maximum retry attempts reached without success")

	// ErrZeroAttempts is returned if MaxAttempts is configured to <= 0.
	ErrZeroAttempts = errors.New("retry: max attempts must be at least 1")
)

// JitterStrategy defines how randomness is applied to backoff intervals to prevent thundering herds.
type JitterStrategy int

const (
	// FullJitter draws sleep uniformly at random from [0, backoff].
	// Recommended by AWS Architecture for distributed microservice systems.
	FullJitter JitterStrategy = iota

	// EqualJitter splits sleep between a deterministic base and uniform random:
	// sleep = backoff/2 + rand(0, backoff/2).
	EqualJitter

	// NoJitter applies pure deterministic exponential backoff: sleep = backoff.
	NoJitter
)

// RetryableFunc represents an executable operation that respects context cancellation.
type RetryableFunc[T any] func(ctx context.Context) (T, error)

// RetryPredicate determines whether a given error qualifies for retry.
// Returning false aborts the retry loop immediately (fail-fast on non-transient errors).
type RetryPredicate func(err error) bool

// OnRetryHook is an observer callback triggered before each retry attempt.
type OnRetryHook func(attempt int, err error, nextBackoff time.Duration)

// MaxAttemptsError is returned when all retry attempts fail.
type MaxAttemptsError struct {
	Attempts int
	LastErr  error
}

func (e *MaxAttemptsError) Error() string {
	return fmt.Sprintf("retry: failed after %d attempts, last error: %v", e.Attempts, e.LastErr)
}

func (e *MaxAttemptsError) Unwrap() error {
	return e.LastErr
}

func (e *MaxAttemptsError) Is(target error) bool {
	return target == ErrMaxAttemptsReached
}

type options struct {
	maxAttempts     int
	initialInterval time.Duration
	maxInterval     time.Duration
	multiplier      float64
	jitter          JitterStrategy
	retryIf         RetryPredicate
	onRetry         OnRetryHook
}

// Option configures retrier behavior.
type Option func(*options)

// WithMaxAttempts sets the maximum number of attempts (including initial call).
// Must be >= 1. Defaults to 3.
func WithMaxAttempts(n int) Option {
	return func(o *options) {
		o.maxAttempts = n
	}
}

// WithInitialInterval sets the base backoff interval for the first retry attempt.
// Defaults to 50ms.
func WithInitialInterval(d time.Duration) Option {
	return func(o *options) {
		o.initialInterval = d
	}
}

// WithMaxInterval sets the upper cap for any single backoff sleep interval.
// Defaults to 2s.
func WithMaxInterval(d time.Duration) Option {
	return func(o *options) {
		o.maxInterval = d
	}
}

// WithMultiplier sets the exponential growth factor applied on each successive retry.
// Defaults to 2.0. Must be >= 1.0.
func WithMultiplier(m float64) Option {
	return func(o *options) {
		o.multiplier = m
	}
}

// WithJitter sets the jitter algorithm to prevent thundering herd collisions.
func WithJitter(j JitterStrategy) Option {
	return func(o *options) {
		o.jitter = j
	}
}

// WithRetryIf specifies a predicate to distinguish transient errors from permanent errors.
// If fn(err) returns false, retrying stops immediately.
func WithRetryIf(fn RetryPredicate) Option {
	return func(o *options) {
		o.retryIf = fn
	}
}

// WithOnRetry registers an observer callback called before each retry delay.
func WithOnRetry(hook OnRetryHook) Option {
	return func(o *options) {
		o.onRetry = hook
	}
}

// StatsSnapshot captures telemetry for a Retrier.
type StatsSnapshot struct {
	TotalCalls           uint64
	TotalRetries         uint64
	SuccessfulCalls      uint64
	PermanentFailures    uint64
	TotalBackoffDuration time.Duration
}

// Retrier executes functions with configurable exponential backoff, jitter, and telemetry.
type Retrier[T any] struct {
	opts options

	totalCalls        atomic.Uint64
	totalRetries      atomic.Uint64
	successfulCalls   atomic.Uint64
	permanentFailures atomic.Uint64
	totalBackoffNs    atomic.Uint64
}

// New creates a new generic Retrier with the specified options.
func New[T any](opts ...Option) *Retrier[T] {
	cfg := options{
		maxAttempts:     3,
		initialInterval: 50 * time.Millisecond,
		maxInterval:     2 * time.Second,
		multiplier:      2.0,
		jitter:          FullJitter,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Retrier[T]{
		opts: cfg,
	}
}

// Stats returns an atomic snapshot of retrier telemetry.
func (r *Retrier[T]) Stats() StatsSnapshot {
	return StatsSnapshot{
		TotalCalls:           r.totalCalls.Load(),
		TotalRetries:         r.totalRetries.Load(),
		SuccessfulCalls:      r.successfulCalls.Load(),
		PermanentFailures:    r.permanentFailures.Load(),
		TotalBackoffDuration: time.Duration(r.totalBackoffNs.Load()),
	}
}

// Execute executes fn, retrying on failure according to the configured options.
func (r *Retrier[T]) Execute(ctx context.Context, fn RetryableFunc[T]) (T, error) {
	var zero T
	if r.opts.maxAttempts <= 0 {
		return zero, ErrZeroAttempts
	}

	r.totalCalls.Add(1)

	var lastErr error
	for attempt := 1; attempt <= r.opts.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			r.permanentFailures.Add(1)
			return zero, err
		}

		val, err := fn(ctx)
		if err == nil {
			r.successfulCalls.Add(1)
			return val, nil
		}

		lastErr = err

		// If error is permanent (predicate says not retryable), fail-fast
		if r.opts.retryIf != nil && !r.opts.retryIf(err) {
			r.permanentFailures.Add(1)
			return zero, err
		}

		// If this was the last attempt, do not backoff
		if attempt >= r.opts.maxAttempts {
			break
		}

		r.totalRetries.Add(1)

		// Calculate backoff with jitter
		backoff := r.calculateBackoff(attempt)
		r.totalBackoffNs.Add(uint64(backoff))

		if r.opts.onRetry != nil {
			r.opts.onRetry(attempt, err, backoff)
		}

		// Non-leaking timer sleep respecting context cancellation
		if backoff > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				r.permanentFailures.Add(1)
				return zero, ctx.Err()
			case <-timer.C:
			}
		}
	}

	r.permanentFailures.Add(1)
	return zero, fmt.Errorf("%w: %w", ErrMaxAttemptsReached, &MaxAttemptsError{
		Attempts: r.opts.maxAttempts,
		LastErr:  lastErr,
	})
}

func (r *Retrier[T]) calculateBackoff(attempt int) time.Duration {
	mult := math.Pow(r.opts.multiplier, float64(attempt-1))
	base := float64(r.opts.initialInterval) * mult
	if base > float64(r.opts.maxInterval) {
		base = float64(r.opts.maxInterval)
	}

	baseDuration := time.Duration(base)
	if baseDuration <= 0 {
		return 0
	}

	switch r.opts.jitter {
	case NoJitter:
		return baseDuration

	case EqualJitter:
		half := baseDuration / 2
		if half <= 0 {
			return baseDuration
		}
		randomPart := rand.N(half)
		return half + randomPart

	case FullJitter:
		fallthrough
	default:
		return rand.N(baseDuration)
	}
}

// Do executes fn with the specified retry options.
func Do[T any](ctx context.Context, fn RetryableFunc[T], opts ...Option) (T, error) {
	retrier := New[T](opts...)
	return retrier.Execute(ctx, fn)
}
