semaphore
=========

[![Golang CI](https://github.com/eapache/go-resiliency/actions/workflows/golang-ci.yml/badge.svg)](https://github.com/eapache/go-resiliency/actions/workflows/golang-ci.yml)
[![GoDoc](https://godoc.org/github.com/eapache/go-resiliency/semaphore?status.svg)](https://godoc.org/github.com/eapache/go-resiliency/semaphore)
[![Code of Conduct](https://img.shields.io/badge/code%20of%20conduct-active-blue.svg)](https://eapache.github.io/conduct.html)

The semaphore resiliency pattern for golang.

Creating a semaphore takes two parameters:
- ticket count (how many tickets to give out at once)
- timeout (how long to wait for a ticket if none are currently available)

```go
sem := semaphore.New(3, 1*time.Second)

if err := sem.Acquire(); err != nil {
	// could not acquire semaphore
	return err
}
defer sem.Release()
```

Tickets are granted to waiting goroutines first-come-first-served. To keep a
slow downstream from piling up unbounded numbers of queued goroutines, the
wait queue can optionally be bounded:

```go
sem := semaphore.NewWithMaxWaiters(3, 1*time.Second, 100)

if err := sem.Acquire(); err != nil {
	if err == semaphore.ErrTooManyWaiters {
		// queue full: rejected immediately, back off
	} else {
		// semaphore.ErrNoTickets: waited and timed out, maybe retry
	}
	return err
}
defer sem.Release()
```
