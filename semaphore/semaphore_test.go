package semaphore

import (
	"context"
	"testing"
	"time"
)

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

func TestSemaphoreAcquireCtxCanceled(t *testing.T) {
	sem := New(1, 5*time.Second)

	if err := sem.Acquire(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		errs <- sem.AcquireCtx(ctx)
	}()

	// make sure the waiter is actually blocked before cancelling
	time.Sleep(50 * time.Millisecond)
	cancel()

	start := time.Now()
	select {
	case err := <-errs:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AcquireCtx did not return after context was canceled")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("AcquireCtx took too long to return after cancel: %v", elapsed)
	}

	// the canceled waiter must not have consumed a ticket, so the single
	// held ticket is still the only one in use
	if sem.IsEmpty() {
		t.Error("semaphore should still hold the originally acquired ticket")
	}

	// and that ticket can be released and acquired again normally
	sem.Release()
	if !sem.IsEmpty() {
		t.Error("semaphore should be empty after releasing the held ticket")
	}
	if err := sem.AcquireCtx(context.Background()); err != nil {
		t.Error(err)
	}
	sem.Release()
}

func TestSemaphoreAcquireCtxConcurrentCancel(t *testing.T) {
	const tickets = 2
	const waiters = 5
	sem := New(tickets, 5*time.Second)

	for i := 0; i < tickets; i++ {
		if err := sem.Acquire(); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			errs <- sem.AcquireCtx(ctx)
		}()
	}

	// let every waiter park on the select before cancelling them all
	time.Sleep(50 * time.Millisecond)
	cancel()

	for i := 0; i < waiters; i++ {
		select {
		case err := <-errs:
			if err != context.Canceled {
				t.Errorf("waiter %d: expected context.Canceled, got %v", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("waiter %d did not return after cancel", i)
		}
	}

	// no canceled waiter may have taken a ticket; exactly tickets are still held
	if sem.IsEmpty() {
		t.Error("semaphore should still hold all originally acquired tickets")
	}
	for i := 0; i < tickets; i++ {
		sem.Release()
	}
	if !sem.IsEmpty() {
		t.Error("semaphore should be empty after releasing every held ticket")
	}

	// all tickets must be reusable after the cancellations
	for i := 0; i < tickets; i++ {
		if err := sem.AcquireCtx(context.Background()); err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < tickets; i++ {
		sem.Release()
	}
	if !sem.IsEmpty() {
		t.Error("semaphore should be empty at the end of the test")
	}
}

func TestSemaphoreAcquireCtxAlreadyCanceled(t *testing.T) {
	sem := New(1, 5*time.Second)

	if err := sem.Acquire(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if err := sem.AcquireCtx(ctx); err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("AcquireCtx took too long to return for an already-canceled context: %v", elapsed)
	}

	// the failed attempt must not have consumed a ticket
	if sem.IsEmpty() {
		t.Error("semaphore should still hold the originally acquired ticket")
	}
	sem.Release()
}

func TestSemaphoreAcquireCtxTimeout(t *testing.T) {
	sem := New(1, 100*time.Millisecond)

	if err := sem.Acquire(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := sem.AcquireCtx(context.Background()); err != ErrNoTickets {
		t.Errorf("expected ErrNoTickets, got %v", err)
	}
	if start.Add(100 * time.Millisecond).After(time.Now()) {
		t.Error("AcquireCtx did not honor the configured timeout")
	}

	sem.Release()
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
