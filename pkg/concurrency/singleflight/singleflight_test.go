package singleflight

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleflight_DuplicateSuppression(t *testing.T) {
	g := NewGroup[string]()
	var callCount int32
	blockCh := make(chan struct{})
	enteredFirst := make(chan struct{})
	var once sync.Once

	fn := func() (string, error) {
		atomic.AddInt32(&callCount, 1)
		once.Do(func() {
			close(enteredFirst)
		})
		<-blockCh
		return "expensive-result", nil
	}

	const callers = 10
	var wg sync.WaitGroup
	results := make([]string, callers)
	sharedFlags := make([]bool, callers)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			val, err, shared := g.Do("cache-key-1", fn)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			results[idx] = val
			sharedFlags[idx] = shared
		}(i)
	}

	// Wait until the single in-flight call has definitely started
	<-enteredFirst
	time.Sleep(50 * time.Millisecond) // Allow all other callers to queue up on the in-flight call
	close(blockCh)
	wg.Wait()

	// Verify only 1 real call occurred
	if calls := atomic.LoadInt32(&callCount); calls != 1 {
		t.Fatalf("expected exactly 1 call, got %d", calls)
	}

	for i, res := range results {
		if res != "expensive-result" {
			t.Errorf("caller %d got %s, expected expensive-result", i, res)
		}
	}

	var sharedCount int
	for _, shared := range sharedFlags {
		if shared {
			sharedCount++
		}
	}
	if sharedCount != callers-1 {
		t.Fatalf("expected %d callers to receive shared=true, got %d", callers-1, sharedCount)
	}
}

func TestSingleflight_ErrorPropagation(t *testing.T) {
	g := NewGroup[int]()
	errExpected := errors.New("upstream failure")

	const callers = 5
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err, _ := g.Do("error-key", func() (int, error) {
				time.Sleep(10 * time.Millisecond)
				return 0, errExpected
			})
			if !errors.Is(err, errExpected) {
				t.Errorf("expected %v, got %v", errExpected, err)
			}
		}()
	}
	wg.Wait()
}
