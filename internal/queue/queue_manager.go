package queue

import "sync"

type QueueManager struct {
	mu     sync.Mutex
	queues map[string]*Queue
}

func NewQueueManager() *QueueManager {
	return &QueueManager{queues: make(map[string]*Queue)}
}

func (m *QueueManager) GetOrCreate(name string) *Queue {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.queues[name]
	if !ok {
		q = NewQueue()
		m.queues[name] = q
	}
	return q
}

func (m *QueueManager) EnqueueWithName(queueName string, item []byte) {
	m.GetOrCreate(queueName).Enqueue(item)
}

func (m *QueueManager) DequeueWithName(queueName string) []byte {
	return m.GetOrCreate(queueName).Dequeue()
}
