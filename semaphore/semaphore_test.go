package semaphore

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSemaphoreAcquireRelease(t *testing.T) {
	sem := New(3, 1*time.Second)

	for i := 0; i < 10; i++ {
		if err := sem.Acquire(); err != nil {
			t.Error(err)
		}
		if err := sem.Acquire(); err != nil {
			t.Error(err)
		}
		if err := sem.Acquire(); err != nil {
			t.Error(err)
		}
		sem.Release()
		sem.Release()
		sem.Release()
	}
}

func TestSemaphoreBlockTimeout(t *testing.T) {
	sem := New(1, 200*time.Millisecond)

	if err := sem.Acquire(); err != nil {
		t.Error(err)
	}

	start := time.Now()
	if err := sem.Acquire(); err != ErrNoTickets {
		t.Error(err)
	}
	if start.Add(200 * time.Millisecond).After(time.Now()) {
		t.Error("semaphore did not wait long enough")
	}

	sem.Release()
	if err := sem.Acquire(); err != nil {
		t.Error(err)
	}
}

func TestSemaphoreEmpty(t *testing.T) {
	sem := New(2, 200*time.Millisecond)

	if !sem.IsEmpty() {
		t.Error("semaphore should be empty")
	}

	sem.Acquire()

	if sem.IsEmpty() {
		t.Error("semaphore should not be empty")
	}

	sem.Release()

	if !sem.IsEmpty() {
		t.Error("semaphore should be empty")
	}
}

func TestSemaphoreErrorsDistinct(t *testing.T) {
	if ErrTooManyWaiters == ErrNoTickets {
		t.Fatal("ErrTooManyWaiters and ErrNoTickets must be distinguishable")
	}
	if errors.Is(ErrTooManyWaiters, ErrNoTickets) || errors.Is(ErrNoTickets, ErrTooManyWaiters) {
		t.Fatal("ErrTooManyWaiters and ErrNoTickets must not unwrap to each other")
	}
}

func TestSemaphoreQueueLimit(t *testing.T) {
	const maxWaiters = 100
	const goroutines = 5000
	sem := NewWithMaxWaiters(1, 10*time.Second, maxWaiters)

	// Hold the only ticket so every caller has to queue.
	if err := sem.Acquire(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var acquired, rejected, timedOut int64
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := sem.Acquire(); err {
			case nil:
				atomic.AddInt64(&acquired, 1)
				sem.Release()
			case ErrTooManyWaiters:
				atomic.AddInt64(&rejected, 1)
			case ErrNoTickets:
				atomic.AddInt64(&timedOut, 1)
			default:
				t.Error(err)
			}
		}()
	}

	// The queue fills to exactly its cap and never beyond.
	waitFor(t, "queue to fill", func() bool { return sem.Waiters() == maxWaiters })

	// A latecomer is rejected immediately, not after the timeout.
	start := time.Now()
	if err := sem.Acquire(); err != ErrTooManyWaiters {
		t.Fatalf("expected ErrTooManyWaiters, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("rejection should be immediate, took %v", elapsed)
	}

	// Everyone who didn't get a queue slot is rejected.
	waitFor(t, "rejections", func() bool {
		return atomic.LoadInt64(&rejected) == goroutines-maxWaiters
	})

	// Drain the queue by releasing the held ticket; every queued waiter
	// gets in, nobody times out.
	sem.Release()
	wg.Wait()

	if got := atomic.LoadInt64(&acquired); got != maxWaiters {
		t.Errorf("expected %d queued waiters to acquire, got %d", maxWaiters, got)
	}
	if got := atomic.LoadInt64(&rejected); got != goroutines-maxWaiters {
		t.Errorf("expected %d rejections, got %d", goroutines-maxWaiters, got)
	}
	if got := atomic.LoadInt64(&timedOut); got != 0 {
		t.Errorf("expected no timeouts, got %d", got)
	}
	if got := sem.Waiters(); got != 0 {
		t.Errorf("expected empty queue, got %d waiters", got)
	}
}

func TestSemaphoreFairness(t *testing.T) {
	const waiters = 64
	sem := NewWithMaxWaiters(1, 10*time.Second, waiters)

	// Hold the only ticket so every caller has to queue.
	if err := sem.Acquire(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	order := make([]int, 0, waiters)
	for i := 0; i < waiters; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			sem.Release()
		}()
		// Ensure goroutine i is enqueued before i+1 so the expected
		// grant order is deterministic.
		waitFor(t, "waiter to enqueue", func() bool { return sem.Waiters() == i+1 })
	}

	sem.Release()
	wg.Wait()

	for i, got := range order {
		if got != i {
			t.Fatalf("tickets not granted first-come-first-served: order[%d] = %d, full order %v", i, got, order)
		}
	}
}

func ExampleSemaphore() {
	sem := New(3, 1*time.Second)

	for i := 0; i < 10; i++ {
		go func() {
			if err := sem.Acquire(); err != nil {
				return //could not acquire semaphore
			}
			defer sem.Release()

			// do something semaphore-guarded
		}()
	}
}
