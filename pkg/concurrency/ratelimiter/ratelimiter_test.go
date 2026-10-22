package ratelimiter

import (
	"context"
	"testing"
	"time"
)

func TestTokenBucket_BurstAndRefill(t *testing.T) {
	// Rate: 10 tokens/sec, Capacity: 3 tokens
	tb := NewTokenBucket(10, 3)

	// Consume 3 burst tokens
	if !tb.Allow() || !tb.Allow() || !tb.Allow() {
		t.Fatal("expected 3 burst tokens to be allowed")
	}

	// 4th token immediately should be denied
	if tb.Allow() {
		t.Fatal("expected 4th immediate token to be rejected")
	}

	// Wait 150ms -> should replenish at least 1 token (10 tokens/sec * 0.15s = 1.5 tokens)
	time.Sleep(150 * time.Millisecond)
	if !tb.Allow() {
		t.Fatal("expected token to be replenished after sleep")
	}
}

func TestTokenBucket_Wait(t *testing.T) {
	// Rate: 50 tokens/sec, Capacity: 1 token
	tb := NewTokenBucket(50, 1)

	// Consume initial token
	if !tb.Allow() {
		t.Fatal("initial allow failed")
	}

	// Wait for next token
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := tb.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	if time.Since(start) < 15*time.Millisecond {
		t.Fatal("wait returned too quickly without throttling")
	}
}
