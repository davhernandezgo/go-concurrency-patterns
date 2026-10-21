package singleton

import (
	"sync"
	"testing"
)

func TestSingleton_ThreadSafety(t *testing.T) {
	const goroutines = 100
	var wg sync.WaitGroup
	instances := make([]*DatabaseConnection, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			instances[idx] = GetInstance()
		}(i)
	}
	wg.Wait()

	first := instances[0]
	if first == nil {
		t.Fatal("expected non-nil instance")
	}

	for i := 1; i < goroutines; i++ {
		if instances[i] != first {
			t.Fatalf("instance at %d differs from instance at 0: pointer mismatch", i)
		}
	}
}
