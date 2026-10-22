package circuitbreaker

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker_Lifecycle(t *testing.T) {
	errMock := errors.New("remote service down")
	cb := New(Config{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          30 * time.Millisecond,
	})

	if cb.State() != StateClosed {
		t.Fatalf("expected state CLOSED, got %s", cb.State())
	}

	// First failure: still Closed
	_, _ = Execute(cb, func() (string, error) {
		return "", errMock
	})
	if cb.State() != StateClosed {
		t.Fatalf("expected state CLOSED after 1 failure, got %s", cb.State())
	}

	// Second failure: trips to Open
	_, _ = Execute(cb, func() (string, error) {
		return "", errMock
	})
	if cb.State() != StateOpen {
		t.Fatalf("expected state OPEN after 2 failures, got %s", cb.State())
	}

	// Execution while Open should be rejected immediately with ErrCircuitOpen
	_, err := Execute(cb, func() (string, error) {
		return "should not run", nil
	})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}

	// Wait for cooldown timeout
	time.Sleep(60 * time.Millisecond)

	// State transitions to Half-Open on next access
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected state HALF_OPEN after timeout, got %s", cb.State())
	}

	// First success in Half-Open: stays Half-Open
	val, err := Execute(cb, func() (string, error) {
		return "success-1", nil
	})
	if err != nil || val != "success-1" {
		t.Fatalf("unexpected error: %v", err)
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected state HALF_OPEN after 1 success, got %s", cb.State())
	}

	// Second success in Half-Open: closes circuit back to Closed
	val, err = Execute(cb, func() (string, error) {
		return "success-2", nil
	})
	if err != nil || val != "success-2" {
		t.Fatalf("unexpected error: %v", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected state CLOSED after recovering, got %s", cb.State())
	}
}

func BenchmarkCircuitBreaker_Execute_ClosedState(b *testing.B) {
	cb := New(Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          1 * time.Second,
	})

	fn := func() (int, error) {
		return 42, nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = Execute(cb, fn)
	}
}

func BenchmarkCircuitBreaker_Execute_Parallel(b *testing.B) {
	cb := New(Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          1 * time.Second,
	})

	fn := func() (int, error) {
		return 42, nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Execute(cb, fn)
		}
	})
}
