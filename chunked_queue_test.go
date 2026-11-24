package lane

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewChunkedQueue(t *testing.T) {
	t.Parallel()

	t.Run("defaults chunk size when zero is provided", func(t *testing.T) {
		t.Parallel()

		queue := NewChunkedQueue[int](0)
		assert.Equal(t, DefaultChunkSize, queue.Capacity())
		assert.True(t, queue.Empty())
	})

	t.Run("enqueues initial items in FIFO order", func(t *testing.T) {
		t.Parallel()

		queue := NewChunkedQueue[int](2, 1, 2, 3)
		assert.Equal(t, uint(3), queue.Size())

		for _, want := range []int{1, 2, 3} {
			got, ok := queue.Dequeue()
			assert.True(t, ok)
			assert.Equal(t, want, got)
		}

		_, ok := queue.Dequeue()
		assert.False(t, ok)
	})
}

func TestChunkedQueueFIFOAcrossGrowth(t *testing.T) {
	t.Parallel()

	// A tiny chunk size forces frequent growth. Dequeuing first keeps a
	// non-zero head offset when growth copies the live items.
	queue := NewChunkedQueue[int](2)

	const total = 100
	dequeueCount := 0
	for value := 0; value < total; value++ {
		queue.Enqueue(value)
		assert.LessOrEqual(t, queue.Size(), queue.Capacity())

		if value%3 == 0 {
			got, ok := queue.Dequeue()
			require.True(t, ok)
			assert.Equal(t, dequeueCount, got)
			dequeueCount++
		}
	}

	for want := 0; want < total; want++ {
		if want < dequeueCount {
			continue
		}

		got, ok := queue.Dequeue()
		require.True(t, ok, "item %d must still be readable after growth", want)
		assert.Equal(t, want, got)
	}

	assert.True(t, queue.Empty())

	// The queue remains usable after draining resets the absolute indices.
	queue.Enqueue(101)
	got, ok := queue.Dequeue()
	require.True(t, ok)
	assert.Equal(t, 101, got)
}

func TestChunkedQueueCapacityDoubles(t *testing.T) {
	t.Parallel()

	queue := NewChunkedQueue[int](4)

	expectedCapacity := uint(4)
	for value := 0; value < 100; value++ {
		queue.Enqueue(value)

		for queue.Capacity() < queue.Size() { // never expected, sanity guard
			t.Fatalf("capacity %d is lower than size %d", queue.Capacity(), queue.Size())
		}

		for queue.Capacity() > expectedCapacity {
			expectedCapacity *= 2
		}
		assert.Equal(t, expectedCapacity, queue.Capacity())
	}

	assert.Greater(t, queue.Capacity(), uint(4))
}

// TestChunkedQueueConcurrentEnqueueDuringGrowth reproduces the failure mode
// where writes keep arriving while a full queue grows: hundreds of items land
// during growth windows. Every enqueued item must be stored once and read back
// intact, with nothing written outside an allocated chunk.
func TestChunkedQueueConcurrentEnqueueDuringGrowth(t *testing.T) {
	t.Parallel()

	const (
		chunkSize   = uint(4)
		producers   = 16
		perProducer = 250
		totalItems  = producers * perProducer
	)

	queue := NewChunkedQueue[int](chunkSize)

	var producersWG sync.WaitGroup

	producersWG.Add(producers)
	for producer := 0; producer < producers; producer++ {
		producer := producer

		go func() {
			defer producersWG.Done()

			for offset := 0; offset < perProducer; offset++ {
				queue.Enqueue(producer*perProducer + offset)
			}
		}()
	}

	// While growth windows open and close, the capacity accounting must stay
	// consistent with the actual chunk layout.
	stopMonitor := make(chan struct{})
	var monitorWG sync.WaitGroup
	monitorWG.Add(1)

	go func() {
		defer monitorWG.Done()

		for {
			select {
			case <-stopMonitor:
				return
			default:
				assert.LessOrEqual(t, queue.Size(), queue.Capacity())
			}
		}
	}()

	producersWG.Wait()
	close(stopMonitor)
	monitorWG.Wait()

	require.Equal(t, uint(totalItems), queue.Size())
	assert.GreaterOrEqual(t, queue.Capacity(), queue.Size())

	seen := make(map[int]struct{}, totalItems)
	for dequeued := 0; dequeued < totalItems; dequeued++ {
		value, ok := queue.Dequeue()
		require.True(t, ok, "dequeue %d failed: an item was lost during growth", dequeued)
		assert.GreaterOrEqual(t, value, 0)
		assert.Less(t, value, totalItems, "read foreign data from outside the chunks")
		assert.NotContains(t, seen, value, "item %d read more than once", value)

		seen[value] = struct{}{}
	}

	assert.Len(t, seen, totalItems)

	_, ok := queue.Dequeue()
	assert.False(t, ok)
	assert.True(t, queue.Empty())
}

// TestChunkedQueueConcurrentProducersAndConsumers keeps producers and
// consumers active at the same time so growth happens while reads are in
// flight as well.
func TestChunkedQueueConcurrentProducersAndConsumers(t *testing.T) {
	t.Parallel()

	const (
		chunkSize   = uint(8)
		producers   = 8
		perProducer = 200
		totalItems  = producers * perProducer
	)

	queue := NewChunkedQueue[int](chunkSize)

	var remaining atomic.Int64
	remaining.Store(totalItems)
	var workersWG sync.WaitGroup

	workersWG.Add(producers)
	for producer := 0; producer < producers; producer++ {
		producer := producer

		go func() {
			defer workersWG.Done()

			for offset := 0; offset < perProducer; offset++ {
				queue.Enqueue(producer*perProducer + offset)
			}
		}()
	}

	seen := make(map[int]struct{}, totalItems)
	var seenMu sync.Mutex

	consumers := 8
	workersWG.Add(consumers)
	for consumer := 0; consumer < consumers; consumer++ {
		go func() {
			defer workersWG.Done()

			for {
				if remaining.Load() == 0 {
					return
				}

				value, ok := queue.Dequeue()
				if !ok {
					continue
				}

				remaining.Add(-1)

				seenMu.Lock()
				seen[value] = struct{}{}
				seenMu.Unlock()
			}
		}()
	}

	workersWG.Wait()

	assert.Len(t, seen, totalItems)
	for value := range seen {
		assert.GreaterOrEqual(t, value, 0)
		assert.Less(t, value, totalItems)
	}

	assert.True(t, queue.Empty())
}
