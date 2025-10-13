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

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
