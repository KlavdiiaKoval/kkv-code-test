package queue

import (
	"sync"
)

type Queue struct {
	mu    sync.Mutex
	items [][]byte
}

func NewQueue() *Queue { return &Queue{} }

func (q *Queue) Enqueue(item []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	copied := make([]byte, len(item))
	copy(copied, item)
	q.items = append(q.items, copied)
}

func (q *Queue) Dequeue() []byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	item := q.items[0]
	if len(q.items) == 1 {
		q.items = q.items[:0]
	} else {
		q.items = q.items[1:]
	}
	return item
}

// item := q.items[0] gets the first element.
// If there’s only one item, q.items = q.items[:0] resets the slice to empty (efficient, no allocation).
// Otherwise, q.items = q.items[1:] drops the first element by slicing (also efficient,
// but the underlying array still holds the old reference until all elements are removed or the slice is reallocated).

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
