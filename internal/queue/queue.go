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

type QueueManager struct {
	mu     sync.Mutex
	queues map[string]*Queue
}

func NewQueueManager() *QueueManager { return &QueueManager{queues: make(map[string]*Queue)} }

func (m *QueueManager) Get(name string) *Queue {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[name]
	if q == nil {
		q = NewQueue()
		m.queues[name] = q
	}
	return q
}
