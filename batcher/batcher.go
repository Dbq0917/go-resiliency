// Package batcher implements the batching resiliency pattern for Go.
package batcher

import (
	"context"
	"sync"
	"time"
)

type work struct {
	param  interface{}
	future chan error
}

// Batcher implements the batching resiliency pattern
type Batcher struct {
	timeout      time.Duration
	maxBatchSize int
	prefilter    func(interface{}) error

	lock         sync.Mutex
	pending      []*work
	doWork       func([]interface{}) error
	batchCounter sync.WaitGroup
	flushTimer   *time.Timer
	batchID      uint64
}

// New constructs a new batcher that will batch all calls to Run that occur within
// `timeout` time before calling doWork just once for the entire batch. The doWork
// function must be safe to run concurrently with itself as this may occur, especially
// when the doWork function is slow, or the timeout is small.
func New(timeout time.Duration, doWork func([]interface{}) error) *Batcher {
	return &Batcher{
		timeout: timeout,
		doWork:  doWork,
	}
}

// WithMaxBatchSize caps the number of parameters passed to doWork in a single
// invocation. A batch is flushed as soon as it reaches this size rather than
// waiting for the timeout to expire. A value less than or equal to zero means
// no limit. This option cannot safely be changed once Run has been invoked.
func (b *Batcher) WithMaxBatchSize(size int) *Batcher {
	b.maxBatchSize = size
	return b
}

// SetMaxBatchSize is an alias for WithMaxBatchSize.
func (b *Batcher) SetMaxBatchSize(size int) *Batcher {
	return b.WithMaxBatchSize(size)
}

// Run runs the work function with the given parameter, possibly
// including it in a batch with other calls to Run that occur within the
// specified timeout. It is safe to call Run concurrently on the same batcher.
func (b *Batcher) Run(param interface{}) error {
	return b.RunCtx(context.Background(), param)
}

// RunCtx is like Run, but stops waiting for the batch to finish if ctx is
// cancelled or its deadline expires. The work may still be included in a batch
// that executes later; RunCtx only controls how long the caller waits.
// It is safe to call RunCtx concurrently on the same batcher.
func (b *Batcher) RunCtx(ctx context.Context, param interface{}) error {
	if b.prefilter != nil {
		if err := b.prefilter(param); err != nil {
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if b.timeout == 0 {
		return b.doWork([]interface{}{param})
	}

	w := &work{
		param:  param,
		future: make(chan error, 1),
	}

	b.submitWork(w)

	select {
	case ret := <-w.future:
		return ret
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RunWithTimeout is a convenience wrapper around RunCtx.
func (b *Batcher) RunWithTimeout(param interface{}, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return b.RunCtx(ctx, param)
}

// Prefilter specifies an optional function that can be used to run initial checks on parameters
// passed to Run before being added to the batch. If the prefilter returns a non-nil error,
// that error is returned immediately from Run and the batcher is not invoked. A prefilter
// cannot safely be specified for a batcher if Run has already been invoked. The filter function
// specified must be concurrency-safe.
func (b *Batcher) Prefilter(filter func(interface{}) error) {
	b.prefilter = filter
}

func (b *Batcher) submitWork(w *work) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if len(b.pending) == 0 {
		b.batchID++
		batchID := b.batchID
		b.flushTimer = time.AfterFunc(b.timeout, func() {
			b.flushTimerFired(batchID)
		})
	}

	b.pending = append(b.pending, w)

	if b.maxBatchSize > 0 && len(b.pending) >= b.maxBatchSize {
		b.flushLocked()
	}
}

func (b *Batcher) flushTimerFired(batchID uint64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.batchID != batchID || len(b.pending) == 0 {
		return
	}

	b.flushLocked()
}

// flushLocked starts a new batch with the currently pending work. The caller
// must hold b.lock.
func (b *Batcher) flushLocked() {
	if len(b.pending) == 0 {
		if b.flushTimer != nil {
			b.flushTimer.Stop()
			b.flushTimer = nil
		}
		return
	}

	if b.flushTimer != nil {
		b.flushTimer.Stop()
		b.flushTimer = nil
	}

	params := make([]interface{}, len(b.pending))
	futures := make([]chan error, len(b.pending))
	for i, w := range b.pending {
		params[i] = w.param
		futures[i] = w.future
	}
	b.pending = nil

	b.batchCounter.Add(1)
	go func() {
		defer b.batchCounter.Done()

		ret := b.doWork(params)
		for _, future := range futures {
			future <- ret
			close(future)
		}
	}()
}

// Shutdown flushes and executes any pending batches. If wait is true, it also waits for the pending batches
// to finish executing before it returns. This can be used to avoid waiting for the timeout to expire when
// gracefully shutting down your application. Calling Run at any point after calling Shutdown will lead to
// undefined behaviour.
func (b *Batcher) Shutdown(wait bool) {
	b.flushCurrentBatch()

	if wait {
		b.batchCounter.Wait()
	}
}

func (b *Batcher) flushCurrentBatch() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.flushLocked()
}
