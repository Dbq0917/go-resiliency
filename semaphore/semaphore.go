// Package semaphore implements the semaphore resiliency pattern for Go.
package semaphore

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

// ErrNoTickets is the error returned by Acquire when it could not acquire
// a ticket from the semaphore within the configured timeout.
var ErrNoTickets = errors.New("could not acquire semaphore ticket")

// ErrTooManyWaiters is the error returned by Acquire when the semaphore's
// wait queue is already full. Unlike ErrNoTickets it is returned immediately
// rather than after a timeout, so callers can distinguish "rejected, back
// off" from "waited and timed out, maybe retry".
var ErrTooManyWaiters = errors.New("too many waiters for semaphore")

// waiter represents one goroutine queued for a ticket. The ready channel is
// buffered so the granter never blocks, and granted is set (under the
// semaphore's lock) before the grant is sent so a concurrently expiring
// timeout can tell the two outcomes apart.
type waiter struct {
	ready   chan struct{}
	granted bool
}

// Semaphore implements the semaphore resiliency pattern
type Semaphore struct {
	timeout    time.Duration
	maxWaiters int

	mu      sync.Mutex
	tickets int
	total   int
	waiters *list.List // FIFO queue of *waiter
}

// New constructs a new Semaphore with the given ticket-count
// and timeout. The wait queue is unbounded; use NewWithMaxWaiters to
// bound it.
func New(tickets int, timeout time.Duration) *Semaphore {
	return NewWithMaxWaiters(tickets, timeout, -1)
}

// NewWithMaxWaiters constructs a new Semaphore with the given ticket-count,
// timeout, and a maximum number of goroutines allowed to queue for a ticket.
// Once the queue is full, Acquire fails immediately with ErrTooManyWaiters
// instead of blocking. A negative maxWaiters means the queue is unbounded.
func NewWithMaxWaiters(tickets int, timeout time.Duration, maxWaiters int) *Semaphore {
	return &Semaphore{
		timeout:    timeout,
		maxWaiters: maxWaiters,
		tickets:    tickets,
		total:      tickets,
		waiters:    list.New(),
	}
}

// Acquire tries to acquire a ticket from the semaphore. If it can, it returns nil.
// If the wait queue is full it returns ErrTooManyWaiters immediately. Otherwise
// it queues (first-come-first-served) and waits; if no ticket arrives within
// "timeout" amount of time, it returns ErrNoTickets. It is safe to call
// Acquire concurrently on a single Semaphore.
func (s *Semaphore) Acquire() error {
	s.mu.Lock()
	if s.tickets > 0 && s.waiters.Len() == 0 {
		s.tickets--
		s.mu.Unlock()
		return nil
	}
	if s.maxWaiters >= 0 && s.waiters.Len() >= s.maxWaiters {
		s.mu.Unlock()
		return ErrTooManyWaiters
	}
	w := &waiter{ready: make(chan struct{}, 1)}
	elem := s.waiters.PushBack(w)
	s.mu.Unlock()

	timer := time.NewTimer(s.timeout)
	defer timer.Stop()

	select {
	case <-w.ready:
		return nil
	case <-timer.C:
		s.mu.Lock()
		defer s.mu.Unlock()
		if w.granted {
			// A ticket was handed to us just as we timed out.
			return nil
		}
		s.waiters.Remove(elem)
		return ErrNoTickets
	}
}

// Release releases an acquired ticket back to the semaphore. It is safe to call
// Release concurrently on a single Semaphore. It is an error to call Release on
// a Semaphore from which you have not first acquired a ticket.
func (s *Semaphore) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem := s.waiters.Front(); elem != nil {
		// Hand the ticket directly to the longest-waiting goroutine.
		w := elem.Value.(*waiter)
		s.waiters.Remove(elem)
		w.granted = true
		w.ready <- struct{}{}
		return
	}
	s.tickets++
}

// IsEmpty will return true if no tickets are being held at that instant.
// It is safe to call concurrently with Acquire and Release, though do note
// that the result may then be unpredictable.
func (s *Semaphore) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tickets == s.total
}

// Waiters returns the number of goroutines currently queued waiting for a
// ticket. It is safe to call concurrently with Acquire and Release, though
// do note that the result may then be unpredictable.
func (s *Semaphore) Waiters() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.waiters.Len()
}
