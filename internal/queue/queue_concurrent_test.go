package queue

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue_Concurrency(t *testing.T) {
	q := NewQueue()
	var wg sync.WaitGroup
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c")}

	// Enqueue concurrently
	for _, item := range items {
		wg.Add(1)
		go func(it []byte) {
			defer wg.Done()
			q.Enqueue(it)
		}(item)
	}
	wg.Wait()
	assert.Equal(t, 3, q.Len())

	// Dequeue concurrently
	results := make(chan string, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := q.Dequeue()
			results <- string(v)
		}()
	}
	wg.Wait()
	close(results)

	var got []string
	for s := range results {
		got = append(got, s)
	}
	assert.ElementsMatch(t, []string{"a", "b", "c"}, got)
	assert.Equal(t, 0, q.Len())
}
