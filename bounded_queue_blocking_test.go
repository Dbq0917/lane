package lane_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oleiade/lane/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Twenty producers hammer one bounded Queue whose upper bound is 100000.
// The size must never exceed the capacity, every item must be consumed, and
// Close must release producers blocked on a full Queue.
func TestBoundedQueue_TwentyProducersRespectLimit(t *testing.T) {
	t.Parallel()

	const (
		capacity    = 100000
		producers   = 20
		perProducer = 10000
		total       = producers * perProducer
	)

	queue := lane.NewBoundedQueue[int](capacity)

	var maxObserved int64
	stopMonitor := make(chan struct{})
	monitorDone := make(chan struct{})

	go func() {
		defer close(monitorDone)

		ticker := time.NewTicker(250 * time.Microsecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopMonitor:
				return
			case <-ticker.C:
				size := int64(queue.Size())

				for {
					previous := atomic.LoadInt64(&maxObserved)
					if size <= previous || atomic.CompareAndSwapInt64(&maxObserved, previous, size) {
						break
					}
				}
			}
		}
	}()

	var consumed int64

	consumersDone := make(chan struct{})

	go func() {
		var waitGroup sync.WaitGroup

		for consumer := 0; consumer < 8; consumer++ {
			waitGroup.Add(1)

			go func() {
				defer waitGroup.Done()

				for {
					if _, ok := queue.Take(); !ok {
						return
					}

					atomic.AddInt64(&consumed, 1)
				}
			}()
		}

		waitGroup.Wait()
		close(consumersDone)
	}()

	var producersWG sync.WaitGroup
	producersWG.Add(producers)

	for producer := 0; producer < producers; producer++ {
		producer := producer

		go func() {
			defer producersWG.Done()

			for offset := 0; offset < perProducer; offset++ {
				if !queue.Enqueue(producer*perProducer + offset) {
					return // closed
				}
			}
		}()
	}

	producersWG.Wait()
	require.NoError(t, queue.Close())

	select {
	case <-consumersDone:
	case <-time.After(30 * time.Second):
		t.Fatal("consumers did not stop after Close")
	}

	close(stopMonitor)
	<-monitorDone

	assert.Equal(t, int64(total), atomic.LoadInt64(&consumed))
	assert.LessOrEqual(t, atomic.LoadInt64(&maxObserved), int64(capacity))
	assert.LessOrEqual(t, int64(queue.Size()), int64(capacity))
}

// With no consumer, exactly capacity items are accepted; every extra
// producer blocks until Close and then returns false instead of hanging.
func TestBoundedQueue_CloseReleasesBlockedProducers(t *testing.T) {
	t.Parallel()

	const (
		capacity  = 100000
		producers = 20
	)

	queue := lane.NewBoundQueue[int](uint(capacity))

	var accepted int64

	var waitGroup sync.WaitGroup
	waitGroup.Add(producers)

	for producer := 0; producer < producers; producer++ {
		go func(base int) {
			defer waitGroup.Done()

			for offset := 0; ; offset++ {
				if queue.Enqueue(base*1_000_000 + offset) {
					atomic.AddInt64(&accepted, 1)
					continue
				}

				return // closed
			}
		}(producer)
	}

	require.Eventually(t, func() bool {
		return queue.Size() == capacity
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, queue.Close())

	done := make(chan struct{})
	go func() {
		waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("blocked producers did not return after Close")
	}

	assert.Equal(t, int64(capacity), atomic.LoadInt64(&accepted))
	assert.Equal(t, uint(capacity), queue.Size())
	assert.False(t, queue.Enqueue(1))
}

func TestBoundedQueue_EnqueueBlocksUntilRoom(t *testing.T) {
	t.Parallel()

	queue := lane.NewBoundedQueue[int](2)
	require.True(t, queue.Enqueue(1))
	require.True(t, queue.Enqueue(2))

	inserted := make(chan struct{})

	go func() {
		if queue.Enqueue(3) {
			close(inserted)
		}
	}()

	select {
	case <-inserted:
		t.Fatal("Enqueue into a full Queue must block")
	case <-time.After(50 * time.Millisecond):
	}

	first, ok := queue.Dequeue()
	require.True(t, ok)
	assert.Equal(t, 1, first)

	select {
	case <-inserted:
	case <-time.After(5 * time.Second):
		t.Fatal("Enqueue was not woken by Dequeue")
	}

	for _, expected := range []int{2, 3} {
		value, ok := queue.Dequeue()
		require.True(t, ok)
		assert.Equal(t, expected, value)
	}
}

func TestBoundedQueue_CloseWakesTakersAndDrains(t *testing.T) {
	t.Parallel()

	queue := lane.NewBoundedQueue[int](4)
	require.True(t, queue.Enqueue(1))
	require.True(t, queue.Enqueue(2))

	var waitGroup sync.WaitGroup
	results := make(chan int, 4)

	for i := 0; i < 3; i++ {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			if value, ok := queue.Take(); ok {
				results <- value
			}
		}()
	}

	require.NoError(t, queue.Close())

	done := make(chan struct{})
	go func() {
		waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Take calls did not return after Close")
	}

	close(results)

	var values []int
	for value := range results {
		values = append(values, value)
	}

	assert.ElementsMatch(t, []int{1, 2}, values)

	_, ok := queue.Take()
	assert.False(t, ok)

	_, ok = queue.Dequeue()
	assert.False(t, ok)
}

func TestBoundedQueue_FIFOAndTryEnqueue(t *testing.T) {
	t.Parallel()

	queue := lane.NewBoundedQueue[int](3, 1, 2, 3)
	assert.True(t, queue.Full())
	assert.False(t, queue.TryEnqueue(99))

	value, ok := queue.Dequeue()
	require.True(t, ok)
	assert.Equal(t, 1, value)

	require.True(t, queue.TryEnqueue(4))
	assert.False(t, queue.Offer(5)) // already back at capacity
	assert.True(t, queue.Full())

	for _, expected := range []int{2, 3, 4} {
		value, ok := queue.Dequeue()
		require.True(t, ok)
		assert.Equal(t, expected, value)
	}

	assert.True(t, queue.Empty())
}

// The historical unbounded constructor keeps accepting items without any
// capacity constraint.
func TestQueue_UnboundedUsageUnaffected(t *testing.T) {
	t.Parallel()

	queue := lane.NewQueue[int]()

	for i := 0; i < 200000; i++ {
		queue.Enqueue(i)
	}

	assert.Equal(t, uint(200000), queue.Size())
	assert.False(t, queue.Full())
	assert.Equal(t, uint(0), queue.Capacity())

	value, ok := queue.Dequeue()
	require.True(t, ok)
	assert.Equal(t, 0, value)
}
