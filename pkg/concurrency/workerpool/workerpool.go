package workerpool

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrPoolStopped = errors.New("worker pool has already been stopped")
	ErrPoolFull    = errors.New("worker pool buffer is full")
)

// Result represents the outcome of a processed job.
type Result[R any] struct {
	Value R
	Err   error
}

// Handler defines the function signature for executing a unit of work.
type Handler[T any, R any] func(ctx context.Context, item T) (R, error)

// Pool manages a pool of concurrent workers processing items from an input queue.
type Pool[T any, R any] struct {
	workers    int
	jobQueue   chan T
	resultChan chan Result[R]
	handler    Handler[T, R]
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	stopOnce   sync.Once
	isStopped  bool
	mu         sync.RWMutex
}

// New creates a new Worker Pool with specified worker count, queue capacity, and task handler.
func New[T any, R any](workers int, queueSize int, handler Handler[T, R]) *Pool[T, R] {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = workers * 2
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Pool[T, R]{
		workers:    workers,
		jobQueue:   make(chan T, queueSize),
		resultChan: make(chan Result[R], queueSize),
		handler:    handler,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start spawns the worker goroutines.
func (p *Pool[T, R]) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// worker is the worker routine pulling jobs from the queue until closed or context cancelled.
func (p *Pool[T, R]) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case item, ok := <-p.jobQueue:
			if !ok {
				return
			}
			val, err := p.handler(p.ctx, item)
			select {
			case <-p.ctx.Done():
				return
			case p.resultChan <- Result[R]{Value: val, Err: err}:
			}
		}
	}
}

// Submit queues an item for processing. It blocks if the queue is full until context is cancelled or space opens up.
func (p *Pool[T, R]) Submit(ctx context.Context, item T) error {
	p.mu.RLock()
	if p.isStopped {
		p.mu.RUnlock()
		return ErrPoolStopped
	}
	p.mu.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	case p.jobQueue <- item:
		return nil
	}
}

// TrySubmit attempts to queue an item without blocking.
func (p *Pool[T, R]) TrySubmit(item T) error {
	p.mu.RLock()
	if p.isStopped {
		p.mu.RUnlock()
		return ErrPoolStopped
	}
	p.mu.RUnlock()

	select {
	case p.jobQueue <- item:
		return nil
	default:
		return ErrPoolFull
	}
}

// Results returns the read-only channel where results are published.
func (p *Pool[T, R]) Results() <-chan Result[R] {
	return p.resultChan
}

// Stop gracefully stops the pool: closes job queue, waits for all active jobs to complete, and closes result channel.
func (p *Pool[T, R]) Stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.isStopped = true
		p.mu.Unlock()

		close(p.jobQueue)
		p.wg.Wait()
		close(p.resultChan)
		p.cancel()
	})
}

// StopNow immediately cancels context and terminates workers without waiting for queued jobs.
func (p *Pool[T, R]) StopNow() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.isStopped = true
		p.mu.Unlock()

		p.cancel()
		p.wg.Wait()
		close(p.resultChan)
	})
}
