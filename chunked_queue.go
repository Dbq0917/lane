package lane

import "sync"

// DefaultChunkSize is the number of items each storage chunk of a
// ChunkedQueue holds when no explicit chunk size is provided.
const DefaultChunkSize uint = 64

// ChunkedQueue is a First In First Out data structure backed by fixed-size
// storage chunks.
//
// Capacity grows on demand: once every allocated slot is occupied, the
// ChunkedQueue allocates twice as many chunks and copies the live items over.
//
// The fullness check, the growth (allocation, copy and block publication) and
// the slot write all happen inside a single exclusive critical section. This
// guarantees that:
//   - Capacity and Size can only ever observe a fully published, consistent
//     chunk layout: a reader never sees the stale pre-growth capacity while the
//     writer already targets the new storage, and no write can ever land
//     outside an allocated chunk.
//   - Writers are never rejected or dropped: concurrent Enqueue calls merely
//     serialize on the mutex while growth completes.
type ChunkedQueue[T any] struct {
	sync.RWMutex

	// chunks holds the allocated storage blocks.
	chunks [][]T

	// chunkSize is the fixed number of slots every chunk holds.
	chunkSize uint

	// head is the absolute index of the next item to dequeue.
	head uint

	// tail is the absolute index at which the next Enqueue writes.
	tail uint
}

// NewChunkedQueue produces a new ChunkedQueue whose chunks each hold chunkSize
// slots. A chunkSize of zero falls back to DefaultChunkSize.
func NewChunkedQueue[T any](chunkSize uint, items ...T) *ChunkedQueue[T] {
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}

	queue := &ChunkedQueue[T]{
		chunks:    [][]T{make([]T, chunkSize)},
		chunkSize: chunkSize,
	}

	for _, item := range items {
		queue.Enqueue(item)
	}

	return queue
}

// Enqueue adds an item at the back of the ChunkedQueue. Growth, when needed,
// happens before the slot is written so the write always targets an allocated
// chunk. Enqueue never fails and never drops an item.
func (q *ChunkedQueue[T]) Enqueue(item T) {
	q.Lock()
	defer q.Unlock()

	if q.tail == q.capacityLocked() {
		q.growLocked()
	}

	chunkIndex, slotIndex := q.tail/q.chunkSize, q.tail%q.chunkSize
	q.chunks[chunkIndex][slotIndex] = item
	q.tail++
}

// Dequeue removes and returns the front item of the ChunkedQueue. The boolean
// result is false when the ChunkedQueue is empty.
func (q *ChunkedQueue[T]) Dequeue() (item T, ok bool) {
	q.Lock()
	defer q.Unlock()

	if q.head == q.tail {
		return item, false
	}

	chunkIndex, slotIndex := q.head/q.chunkSize, q.head%q.chunkSize
	item = q.chunks[chunkIndex][slotIndex]

	var zero T
	q.chunks[chunkIndex][slotIndex] = zero // avoid memory leaks
	q.head++

	if q.head == q.tail {
		// Reset absolute indices so they cannot grow without bound over
		// a long-lived queue's lifetime.
		q.head, q.tail = 0, 0
	}

	return item, true
}

// Head returns the front item of the ChunkedQueue without removing it. The
// boolean result is false when the ChunkedQueue is empty.
func (q *ChunkedQueue[T]) Head() (item T, ok bool) {
	q.RLock()
	defer q.RUnlock()

	if q.head == q.tail {
		return item, false
	}

	chunkIndex, slotIndex := q.head/q.chunkSize, q.head%q.chunkSize

	return q.chunks[chunkIndex][slotIndex], true
}

// Size returns the number of items currently stored in the ChunkedQueue.
func (q *ChunkedQueue[T]) Size() uint {
	q.RLock()
	defer q.RUnlock()

	return q.tail - q.head
}

// Empty returns whether the ChunkedQueue holds no item.
func (q *ChunkedQueue[T]) Empty() bool {
	q.RLock()
	defer q.RUnlock()

	return q.head == q.tail
}

// Capacity returns the number of slots currently allocated across every chunk.
// It is always accurate, including while growth is in progress: readers wait
// for the new chunk layout to be fully published before observing it.
func (q *ChunkedQueue[T]) Capacity() uint {
	q.RLock()
	defer q.RUnlock()

	return q.capacityLocked()
}

// capacityLocked returns the allocated slot count. It assumes the caller holds
// at least a read lock.
func (q *ChunkedQueue[T]) capacityLocked() uint {
	return uint(len(q.chunks)) * q.chunkSize
}

// growLocked doubles the number of chunks and copies the live items at the
// front of the new storage. It assumes the caller holds the write lock, so the
// new capacity is published atomically with the new chunk layout.
func (q *ChunkedQueue[T]) growLocked() {
	newChunks := make([][]T, uint(len(q.chunks))*2)
	for index := range newChunks {
		newChunks[index] = make([]T, q.chunkSize)
	}

	size := q.tail - q.head
	for offset := uint(0); offset < size; offset++ {
		source := q.head + offset
		newChunks[offset/q.chunkSize][offset%q.chunkSize] =
			q.chunks[source/q.chunkSize][source%q.chunkSize]
	}

	q.chunks = newChunks
	q.head = 0
	q.tail = size
}
