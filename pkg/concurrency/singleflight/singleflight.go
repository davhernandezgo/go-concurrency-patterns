package singleflight

import (
	"sync"
)

// Result holds the return values from a singleflight execution.
type Result[T any] struct {
	Val    T
	Err    error
	Shared bool
}

type call[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
	ch  chan Result[T]
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group[T any] struct {
	mu sync.Mutex
	m  map[string]*call[T]
}

// NewGroup creates a new generic Singleflight Group.
func NewGroup[T any]() *Group[T] {
	return &Group[T]{
		m: make(map[string]*call[T]),
	}
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// The third return value indicates whether the result was shared with other callers.
func (g *Group[T]) Do(key string, fn func() (T, error)) (T, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call[T])
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := new(call[T])
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	g.doCall(c, key, fn)
	return c.val, c.err, false
}

func (g *Group[T]) doCall(c *call[T], key string, fn func() (T, error)) {
	defer func() {
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()
		c.wg.Done()
	}()

	c.val, c.err = fn()
}
