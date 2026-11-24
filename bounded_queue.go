package lane

import (
	"errors"
	"reflect"
)

// ErrQueueClosed is reported by error-returning helpers when an operation
// cannot complete because the Queue was closed. The boolean-returning
// methods (Enqueue, TryEnqueue, ...) report the same condition with false.
var ErrQueueClosed = errors.New("lane: queue is closed")

// BoundedQueue is a Queue whose number of stored items is capped. Producers
// calling Enqueue are blocked while the Queue is full, and are woken up as
// consumers make room or when the Queue is closed.
//
// BoundedQueue wraps Queue so it exposes the exact same goroutine-safe
// API while carrying the capacity bound.
type BoundedQueue[T any] struct {
	*Queue[T]
}

// BoundQueue is the capacity-aware counterpart of Queue, named after the
// existing BoundDeque type. See BoundedQueue for semantics.
type BoundQueue[T any] struct {
	*Queue[T]
}

// BlockingQueue is an alternative name for a bounded Queue: full queues
// block producers and empty queues may block consumers through Take.
type BlockingQueue[T any] struct {
	*Queue[T]
}

// BoundedBlockingQueue is an explicit alias for callers preferring the
// canonical concurrent-data-structure name.
type BoundedBlockingQueue[T any] struct {
	*Queue[T]
}

// NewBoundedQueue produces a bounded Queue instance holding at most
// capacity items.
//
// The capacity argument accepts any signed or unsigned integer kind (an
// untyped numeric constant such as 100000 works as well). A negative or
// unrecognized capacity falls back to zero, meaning the Queue is always
// full and Enqueue blocks until Close is called. Initial items are inserted
// in FIFO order and are not dropped, even when they outnumber the capacity.
func NewBoundedQueue[T any](capacity any, items ...T) *BoundedQueue[T] {
	return &BoundedQueue[T]{
		Queue: newCapacityQueue[T](normalizeCapacity(capacity), items...),
	}
}

// NewBoundQueue produces a bounded Queue following the NewBoundDeque naming
// and signature convention.
func NewBoundQueue[T any](capacity any, items ...T) *BoundQueue[T] {
	return &BoundQueue[T]{
		Queue: newCapacityQueue[T](normalizeCapacity(capacity), items...),
	}
}

// NewBlockingQueue produces a bounded blocking Queue.
func NewBlockingQueue[T any](capacity any, items ...T) *BlockingQueue[T] {
	return &BlockingQueue[T]{
		Queue: newCapacityQueue[T](normalizeCapacity(capacity), items...),
	}
}

// NewBoundedBlockingQueue produces a bounded blocking Queue.
func NewBoundedBlockingQueue[T any](capacity any, items ...T) *BoundedBlockingQueue[T] {
	return &BoundedBlockingQueue[T]{
		Queue: newCapacityQueue[T](normalizeCapacity(capacity), items...),
	}
}

// NewQueueWithCapacity produces a Queue constrained by the provided
// capacity. It is an alias of NewBoundedQueue for callers extending the
// existing NewQueue constructor family.
func NewQueueWithCapacity[T any](capacity any, items ...T) *Queue[T] {
	return newCapacityQueue[T](normalizeCapacity(capacity), items...)
}

// NewWithCapacity is a short alias of NewQueueWithCapacity.
func NewWithCapacity[T any](capacity any, items ...T) *Queue[T] {
	return newCapacityQueue[T](normalizeCapacity(capacity), items...)
}

func newCapacityQueue[T any](capacity int, items ...T) *Queue[T] {
	if capacity < 0 {
		capacity = 0
	}

	queue := NewQueue[T](items...)
	queue.mu.Lock()
	queue.bounded = true
	queue.capacity = capacity
	queue.mu.Unlock()

	return queue
}

// normalizeCapacity accepts every signed/unsigned integer kind so callers
// passing either int or uint capacities compile and behave identically.
func normalizeCapacity(capacity any) int {
	if capacity == nil {
		return 0
	}

	value := reflect.ValueOf(capacity)
	maxInt := int64(^uint(0) >> 1)

	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := value.Int()
		if n < 0 {
			return 0
		}

		if n > maxInt {
			return int(maxInt)
		}

		return int(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n := value.Uint()
		if n > uint64(maxInt) {
			return int(maxInt)
		}

		return int(n)
	default:
		return 0
	}
}

// Close marks the Queue as closed.
//
// All producers currently blocked in Enqueue (and consumers blocked in Take)
// are woken up: blocked Enqueue calls return false and blocked Take calls
// return ok=false once the Queue is empty. Items already enqueued are not
// discarded and remain available through Dequeue until drained. New Enqueue
// calls on a closed Queue return false immediately instead of blocking.
//
// Close is idempotent and never blocks; its returned error is always nil but
// kept so the Queue satisfies the io.Closer convention.
func (q *Queue[T]) Close() error {
	q.mu.Lock()
	q.ensureReady()
	q.closed = true

	// Broadcast (rather than Signal) because every blocked producer or
	// consumer must observe the closed state and return.
	q.notFull.Broadcast()
	q.notEmpty.Broadcast()
	q.mu.Unlock()

	// Closing the signal channel outside the mutex avoids holding q.mu
	// while waiters selected on Done wake up. sync.Once keeps repeated
	// Close calls panic-free and idempotent.
	q.closeOnce.Do(func() {
		close(q.done)
	})

	return nil
}

// Done returns a channel that is closed when the Queue is closed. It offers
// a selectable shutdown signal complementary to the blocking Enqueue/Take
// methods.
func (q *Queue[T]) Done() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	return q.done
}

// Shutdown is an alias of Close.
func (q *Queue[T]) Shutdown() error {
	return q.Close()
}

// Stop is an alias of Close.
func (q *Queue[T]) Stop() error {
	return q.Close()
}

// Closed reports whether Close has been called.
func (q *Queue[T]) Closed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.closed
}

// IsClosed is an alias of Closed.
func (q *Queue[T]) IsClosed() bool {
	return q.Closed()
}

// Put is an alias of the blocking Enqueue operation. It returns false only
// when the Queue is closed before the item was accepted.
func (q *Queue[T]) Put(item T) bool {
	return q.Enqueue(item)
}

// Send is an alias of the blocking Enqueue operation.
func (q *Queue[T]) Send(item T) bool {
	return q.Enqueue(item)
}

// Push is an alias of the blocking Enqueue operation.
func (q *Queue[T]) Push(item T) bool {
	return q.Enqueue(item)
}

// Add is an alias of the blocking Enqueue operation.
func (q *Queue[T]) Add(item T) bool {
	return q.Enqueue(item)
}

// EnqueueOrError behaves like Enqueue but reports rejection through
// ErrQueueClosed instead of a boolean.
func (q *Queue[T]) EnqueueOrError(item T) error {
	if q.Enqueue(item) {
		return nil
	}

	return ErrQueueClosed
}

// PutOrError is the error-returning counterpart of Put.
func (q *Queue[T]) PutOrError(item T) error {
	return q.EnqueueOrError(item)
}

// TryEnqueue inserts item only when the Queue currently has free capacity.
// It never blocks and reports whether the item was inserted. A closed or
// full Queue makes TryEnqueue return false.
func (q *Queue[T]) TryEnqueue(item T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	if q.closed || q.fullLocked() {
		return false
	}

	q.container.Prepend(item)
	q.notEmpty.Signal()

	return true
}

// Offer is the conventional non-blocking enqueue alias: it inserts the item
// when room is available and returns false otherwise.
func (q *Queue[T]) Offer(item T) bool {
	return q.TryEnqueue(item)
}

// Append is the non-blocking, BoundDeque-compatible insertion alias: it
// appends the item when capacity allows and returns false when the Queue is
// full or closed. Use Enqueue/Put for blocking backpressure.
func (q *Queue[T]) Append(item T) bool {
	return q.TryEnqueue(item)
}

// Prepend mirrors Append for callers used to the BoundDeque API. On a Queue
// it is equivalent to TryEnqueue and preserves FIFO Dequeue ordering.
func (q *Queue[T]) Prepend(item T) bool {
	return q.TryEnqueue(item)
}

// WaitEnqueue is an explicit alias of the blocking Enqueue operation.
func (q *Queue[T]) WaitEnqueue(item T) bool {
	return q.Enqueue(item)
}

// TryPut is an alias of TryEnqueue.
func (q *Queue[T]) TryPut(item T) bool {
	return q.TryEnqueue(item)
}

// TrySend is an alias of TryEnqueue.
func (q *Queue[T]) TrySend(item T) bool {
	return q.TryEnqueue(item)
}

// Take removes and returns the front item, blocking while the Queue is
// empty. It returns ok=false when the Queue is closed and has been drained,
// so a consumer waiting on an empty Queue is never stuck after Close.
func (q *Queue[T]) Take() (item T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	for q.container.Size() == 0 && !q.closed {
		q.notEmpty.Wait()
	}

	item, ok = q.container.Pop()
	if ok {
		q.notFull.Signal()
	}

	return item, ok
}

// WaitDequeue is an alias of the blocking Take operation.
func (q *Queue[T]) WaitDequeue() (item T, ok bool) {
	return q.Take()
}

// Pop is an alias of the non-blocking Dequeue operation.
func (q *Queue[T]) Pop() (item T, ok bool) {
	return q.Dequeue()
}

// Shift is an alias of the non-blocking Dequeue operation.
func (q *Queue[T]) Shift() (item T, ok bool) {
	return q.Dequeue()
}

// Remove is an alias of the non-blocking Dequeue operation.
func (q *Queue[T]) Remove() (item T, ok bool) {
	return q.Dequeue()
}

// Get is an alias of the non-blocking Dequeue operation.
func (q *Queue[T]) Get() (item T, ok bool) {
	return q.Dequeue()
}

// Peek is an alias of Head: it returns the front item without removing it.
func (q *Queue[T]) Peek() (item T, ok bool) {
	return q.Head()
}

// Len returns the current number of items in the Queue as an int.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	return int(q.container.Size())
}

// Size returns the current number of items in the Queue. It preserves the
// historical uint signature shared with Deque and BoundDeque.
func (q *Queue[T]) Size() uint {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	return q.container.Size()
}

// Capacity returns the capacity of a bounded Queue. For an unbounded Queue
// it returns zero.
func (q *Queue[T]) Capacity() uint {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	if !q.bounded {
		return 0
	}

	return uint(q.capacity)
}

// Cap returns the capacity of a bounded Queue as an int. It returns zero
// for an unbounded Queue.
func (q *Queue[T]) Cap() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	if !q.bounded {
		return 0
	}

	return q.capacity
}

// SetCapacity (re)defines the upper bound of a Queue. A non-positive
// capacity is normalized to zero. Growing the capacity wakes producers
// waiting for room; shrinking simply makes them re-evaluate the new limit.
// Calling SetCapacity on a previously unbounded Queue turns it bounded.
func (q *Queue[T]) SetCapacity(capacity int) {
	if capacity < 0 {
		capacity = 0
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()
	q.bounded = true
	q.capacity = capacity

	// The new bound may allow waiters to proceed (growth) or require them
	// to keep waiting (shrink); broadcasting lets every waiter re-check.
	q.notFull.Broadcast()
}

// Empty reports whether the Queue contains no item.
func (q *Queue[T]) Empty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	return q.container.Size() == 0
}

// IsEmpty is an alias of Empty.
func (q *Queue[T]) IsEmpty() bool {
	return q.Empty()
}

// Full reports whether a bounded Queue has reached its capacity. An
// unbounded Queue is never full. A capacity of zero is always full,
// matching BoundDeque.Full.
func (q *Queue[T]) Full() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	return q.fullLocked()
}

// IsFull is an alias of Full.
func (q *Queue[T]) IsFull() bool {
	return q.Full()
}

// Drain removes and returns every currently buffered item in FIFO order.
// It never blocks. Draining frees every occupied slot and therefore wakes
// all producers waiting for room.
func (q *Queue[T]) Drain() []T {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	items := make([]T, 0, q.container.Size())

	for {
		item, ok := q.container.Pop()
		if !ok {
			break
		}

		items = append(items, item)
	}

	q.notFull.Broadcast()

	return items
}
