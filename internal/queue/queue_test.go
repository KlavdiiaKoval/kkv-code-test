package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueueEnqueue(t *testing.T) {
	cases := []struct {
		name   string
		input  [][]byte
		expect [][]byte
	}{
		{"empty", nil, [][]byte{}},
		{"single", [][]byte{[]byte("a")}, [][]byte{[]byte("a")}},
		{"multi", [][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte("a"), []byte("b")}},
		{"nil input", [][]byte{nil}, [][]byte{{}}},
		{"empty slice", [][]byte{{}}, [][]byte{{}}},
		{"large slice", [][]byte{make([]byte, 1024)}, [][]byte{make([]byte, 1024)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := NewQueue()
			for _, b := range c.input {
				q.Enqueue(b)
			}
			got := make([][]byte, 0, len(c.input))
			for i := 0; i < len(c.input); i++ {
				got = append(got, q.Dequeue())
			}
			assert.Equal(t, c.expect, got)
		})
	}
}

func TestQueueDequeue(t *testing.T) {
	cases := []struct {
		name   string
		pushes [][]byte
		pops   int
		expect [][]byte
	}{
		{"empty queue", nil, 1, [][]byte{nil}},
		{"pop all", [][]byte{[]byte("a"), []byte("b")}, 2, [][]byte{[]byte("a"), []byte("b")}},
		{"pop more than present", [][]byte{[]byte("a")}, 3, [][]byte{[]byte("a"), nil, nil}},
		{"nil input", [][]byte{nil}, 1, [][]byte{{}}},
		{"empty slice", [][]byte{{}}, 1, [][]byte{{}}},
		{"large slice", [][]byte{make([]byte, 1024)}, 1, [][]byte{make([]byte, 1024)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := NewQueue()
			for _, b := range c.pushes {
				q.Enqueue(b)
			}
			got := make([][]byte, 0, c.pops)
			for i := 0; i < c.pops; i++ {
				got = append(got, q.Dequeue())
			}
			assert.Equal(t, c.expect, got)
		})
	}
}

func TestQueueLen(t *testing.T) {
	cases := []struct {
		name   string
		pushes [][]byte
		pops   int
		expect int
	}{
		{"empty", nil, 0, 0},
		{"push 2 pop 1", [][]byte{[]byte("a"), []byte("b")}, 1, 1},
		{"push 1 pop 2", [][]byte{[]byte("a")}, 2, 0},
		{"push 5 pop 2", [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}, 2, 3},
		{"push 3 pop 5", [][]byte{[]byte("a"), []byte("b"), []byte("c")}, 5, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := NewQueue()
			for _, b := range c.pushes {
				q.Enqueue(b)
			}
			for i := 0; i < c.pops; i++ {
				q.Dequeue()
			}
			assert.Equal(t, c.expect, q.Len())
		})
	}

}
func TestQueueEnqueueCopiesData(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"short", []byte("abc")},
		{"long", make([]byte, 100)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := NewQueue()
			buf := make([]byte, len(c.data))
			copy(buf, c.data)
			q.Enqueue(buf)
			if len(buf) > 0 {
				buf[0] ^= 0xFF // mutate
			}
			out := q.Dequeue()
			assert.Equal(t, c.data, out)
		})
	}
}

func TestQueueReuseAfterEmpty(t *testing.T) {
	q := NewQueue()
	q.Enqueue([]byte("first"))
	assert.Equal(t, []byte("first"), q.Dequeue())
	assert.Equal(t, 0, q.Len())
	q.Enqueue([]byte("second"))
	assert.Equal(t, 1, q.Len())
	assert.Equal(t, []byte("second"), q.Dequeue())
	assert.Equal(t, 0, q.Len())
}

func TestQueueLargeNumberOfItems(t *testing.T) {
	q := NewQueue()
	n := 1000
	for i := 0; i < n; i++ {
		q.Enqueue([]byte{byte(i % 256)})
	}
	assert.Equal(t, n, q.Len())
	for i := 0; i < n; i++ {
		b := q.Dequeue()
		assert.Equal(t, []byte{byte(i % 256)}, b)
	}
	assert.Equal(t, 0, q.Len())
}

func TestQueueLenAfterEachOperation(t *testing.T) {
	q := NewQueue()
	assert.Equal(t, 0, q.Len())
	q.Enqueue([]byte("a"))
	assert.Equal(t, 1, q.Len())
	q.Enqueue([]byte("b"))
	assert.Equal(t, 2, q.Len())
	q.Dequeue()
	assert.Equal(t, 1, q.Len())
	q.Dequeue()
	assert.Equal(t, 0, q.Len())
	q.Dequeue() // extra dequeue
	assert.Equal(t, 0, q.Len())
}
