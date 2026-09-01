package tools

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithLockRunsActionUnderLock(t *testing.T) {
	var mu sync.Mutex
	called := false

	WithLock(&mu, func() {
		called = true
	})

	require.True(t, called)
}

func TestWithLockHandlesNilAction(t *testing.T) {
	var mu sync.Mutex

	require.NotPanics(t, func() {
		WithLock(&mu, nil)
	})
}

func TestPromiseSetAndGetFuture(t *testing.T) {
	promise := NewPromise[int]()
	future := promise.GetFuture()

	promise.Set(42)

	require.Equal(t, 42, future.Get())
}

func TestPromiseSetIsIdempotent(t *testing.T) {
	promise := NewPromise[int]()
	future := promise.GetFuture()

	promise.Set(1)
	require.NotPanics(t, func() {
		promise.Set(2)
	})
	require.Equal(t, 1, future.Get())
}

func TestNewFutureReturnsValueSentOnChannel(t *testing.T) {
	ch := make(chan string, 1)
	ch <- "value"

	future := NewFuture[string](ch)
	require.Equal(t, "value", future.Get())
}

func TestSemaphoreLimitsConcurrency(t *testing.T) {
	sem := NewSemaphore(1)

	sem.Acquire()

	acquired := make(chan struct{})
	go func() {
		sem.Acquire()
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("second Acquire should block while ticket is held")
	case <-time.After(50 * time.Millisecond):
	}

	sem.Release()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second Acquire should proceed after Release")
	}
}

func TestWithSemaphoreRunsActionAndReleasesTicket(t *testing.T) {
	sem := NewSemaphore(1)
	called := false

	sem.WithSemaphore(func() {
		called = true
	})

	require.True(t, called)

	done := make(chan struct{})
	go func() {
		sem.Acquire()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ticket was not released")
	}
}

func TestWithSemaphoreHandlesNilAction(t *testing.T) {
	sem := NewSemaphore(1)

	require.NotPanics(t, func() {
		sem.WithSemaphore(nil)
	})
}
