package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is OPEN; request rejected")
)

type State string

const (
	StateClosed   State = "CLOSED"
	StateOpen     State = "OPEN"
	StateHalfOpen State = "HALF_OPEN"
)

// Config holds circuit breaker parameters.
type Config struct {
	FailureThreshold    int           // consecutive failures to trip from Closed to Open
	SuccessThreshold    int           // consecutive successes in Half-Open to recover to Closed
	Timeout             time.Duration // cooldown period before transitioning from Open to Half-Open
}

// CircuitBreaker guards operations against cascading failures.
type CircuitBreaker struct {
	config             Config
	state              State
	consecutiveFailures int
	consecutiveSuccess  int
	lastStateChange    time.Time
	mu                 sync.RWMutex
}

// New creates a new CircuitBreaker.
func New(cfg Config) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}

	return &CircuitBreaker{
		config:          cfg,
		state:           StateClosed,
		lastStateChange: time.Now(),
	}
}

// State returns the current circuit state, transitioning Open -> Half-Open if timeout has expired.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateOpen && time.Since(cb.lastStateChange) >= cb.config.Timeout {
		cb.state = StateHalfOpen
		cb.consecutiveSuccess = 0
		cb.lastStateChange = time.Now()
	}
	return cb.state
}

// Execute wraps an operation with circuit breaker protection.
func Execute[T any](cb *CircuitBreaker, fn func() (T, error)) (T, error) {
	var zero T
	currentState := cb.State()

	if currentState == StateOpen {
		return zero, ErrCircuitOpen
	}

	res, err := fn()
	cb.recordResult(err)
	return res, err
}

func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.consecutiveSuccess = 0
		cb.consecutiveFailures++

		if cb.state == StateHalfOpen || cb.consecutiveFailures >= cb.config.FailureThreshold {
			cb.state = StateOpen
			cb.lastStateChange = time.Now()
		}
	} else {
		cb.consecutiveFailures = 0
		if cb.state == StateHalfOpen {
			cb.consecutiveSuccess++
			if cb.consecutiveSuccess >= cb.config.SuccessThreshold {
				cb.state = StateClosed
				cb.lastStateChange = time.Now()
			}
		}
	}
}
