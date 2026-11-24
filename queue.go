package lane

import "sync"

// Queue is a First In First Out data structure implementation.
//
// Built upon a Deque container, its API focuses on the following core
// functionalities: Enqueue, Dequeue, Head, Size, Empty.
//
// Every operation has a time complexity of *O(1)*.
//
// Every operation over an instantiated Queue are goroutine-safe.
type Queue[T any] struct {
	container *Deque[T]

	// mu serializes the capacity check together with the insertion (or
	// removal) it guards. "Is there room?" and the following Enqueue are
	// therefore a single atomic step, closing the check-then-act window
	// that allowed concurrent producers to overshoot the capacity.
	mu sync.Mutex

	// notFull is broadcast/signalled when an item is removed and waiting
	// producers may be able to insert. notEmpty does the symmetric job for
	// blocking consumers. They are pointers so that a Queue is free to be
	// passed around by pointer without copying the embedded sync.Cond.
	notFull  *sync.Cond
	notEmpty *sync.Cond

	// done is closed once by Close and backs Done.
	done      chan struct{}
	closeOnce sync.Once

	// bounded reports whether capacity should be enforced. The zero value
	// leaves a Queue unbounded, which preserves the historical behavior.
	bounded bool

	// capacity is the maximum number of items a bounded Queue may hold. It
	// is only meaningful when bounded is true.
	capacity int

	// closed reports whether Close was called. A closed Queue rejects new
	// Enqueue calls and wakes every producer/consumer blocked on a full or
	// empty Queue so that nobody waits forever. Buffered items remain
	// readable until drained.
	closed bool
}

// NewQueue produces a new Queue instance.
func NewQueue[T any](items ...T) *Queue[T] {
	deque := NewDeque[T]()

	for _, item := range items {
		deque.container.PushFront(item)
	}

	queue := &Queue[T]{
		container: deque,
	}
	queue.initConditions()

	return queue
}

// initConditions wires the condition variables to the Queue mutex. It is
// safe to call more than once and must be called with q.mu held when the
// Queue may already be shared.
func (q *Queue[T]) initConditions() {
	if q.notFull == nil {
		q.notFull = sync.NewCond(&q.mu)
	}

	if q.notEmpty == nil {
		q.notEmpty = sync.NewCond(&q.mu)
	}

	if q.done == nil {
		q.done = make(chan struct{})
	}
}

// ensureReady initializes a zero-value Queue so that even a Queue built as
// new(Queue[T]) is usable. It must be called with q.mu held.
func (q *Queue[T]) ensureReady() {
	if q.container == nil {
		q.container = NewDeque[T]()
	}

	q.initConditions()
}

// fullLocked reports whether a bounded Queue has reached its capacity. It
// must be called with q.mu held.
func (q *Queue[T]) fullLocked() bool {
	return q.bounded && int(q.container.Size()) >= q.capacity
}

// Enqueue adds an item at the back of the Queue in *O(1)* time complexity.
//
// On an unbounded Queue Enqueue never blocks and always reports true. On a
// bounded Queue it blocks while the Queue is full, applying backpressure to
// producers instead of letting the size overshoot its capacity. Enqueue
// returns false when the Queue is closed before the item could be inserted;
// it never blocks forever after Close. The call is safe for concurrent use.
func (q *Queue[T]) Enqueue(item T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	for q.fullLocked() && !q.closed {
		q.notFull.Wait()
	}

	if q.closed {
		return false
	}

	q.container.Prepend(item)
	q.notEmpty.Signal()

	return true
}

// Dequeue removes and returns the Queue's front item in *O(1)* time complexity.
func (q *Queue[T]) Dequeue() (item T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.ensureReady()

	item, ok = q.container.Pop()
	if ok {
		// Frees a slot: wake one producer blocked on a full Queue.
		q.notFull.Signal()
	}

	return item, ok
}

// Head returns the Queue's front queue item in *O(1)* time complexity.
func (q *Queue[T]) Head() (item T, ok bool) {
	return q.container.Last()
}
