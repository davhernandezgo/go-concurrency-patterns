package ratelimiter

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)

// TokenBucket implements a thread-safe token bucket rate limiter.
type TokenBucket struct {
	rate       float64 // tokens added per second
	capacity   float64 // maximum burst capacity
	tokens     float64 // current available tokens
	lastRefill time.Time
	mu         sync.Mutex
}

// NewTokenBucket creates a rate limiter with sustained tokens-per-second rate and max capacity burst.
func NewTokenBucket(rate float64, capacity float64) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
}

// refill updates the available tokens based on elapsed time. Must be called with lock held.
func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = tb.tokens + elapsed*tb.rate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.lastRefill = now
}

// Allow returns true if a token can be consumed immediately, or false otherwise.
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN returns true if n tokens can be consumed immediately.
func (tb *TokenBucket) AllowN(n float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()
	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}
	return false
}

// Wait blocks until a token becomes available or the context is cancelled.
func (tb *TokenBucket) Wait(ctx context.Context) error {
	for {
		tb.mu.Lock()
		tb.refill()
		if tb.tokens >= 1 {
			tb.tokens--
			tb.mu.Unlock()
			return nil
		}

		// Calculate sleep time until next token
		missing := 1.0 - tb.tokens
		waitTime := time.Duration((missing / tb.rate) * float64(time.Second))
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
		}
	}
}
