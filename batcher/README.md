batcher
=======

[![Golang CI](https://github.com/eapache/go-resiliency/actions/workflows/golang-ci.yml/badge.svg)](https://github.com/eapache/go-resiliency/actions/workflows/golang-ci.yml)
[![GoDoc](https://godoc.org/github.com/eapache/go-resiliency/batcher?status.svg)](https://godoc.org/github.com/eapache/go-resiliency/batcher)
[![Code of Conduct](https://img.shields.io/badge/code%20of%20conduct-active-blue.svg)](https://eapache.github.io/conduct.html)

The batching resiliency pattern for golang.

Creating a batcher takes two parameters:
- the timeout to wait while collecting a batch
- the function to run once a batch has been collected

You can also optionally set a prefilter to fail queries before they enter the
batch, and a maximum batch size so that a full batch is dispatched immediately
instead of waiting for the timeout. `RunCtx` lets a caller stop waiting before
the batch is dispatched.

```go
b := batcher.New(10*time.Millisecond, func(params []interface{}) error {
	// do something with the batch of parameters
	return nil
})

b.Prefilter(func(param interface{}) error {
	// do some sort of sanity check on the parameter, and return an error if it fails
	return nil
})

b.WithMaxBatchSize(100)

for i := 0; i < 10; i++ {
	go b.Run(i)
}
```

To bound how long a caller waits for its batch:

```go
ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
defer cancel()

if err := b.RunCtx(ctx, param); err != nil {
	// the work may still be processed later, but this caller has stopped waiting
}
```
