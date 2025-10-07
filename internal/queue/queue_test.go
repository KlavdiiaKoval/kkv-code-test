package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	tests := []struct {
		name   string
		pushes []string
		pops   int
		expect []string
	}{
		{
			name:   "EmptyQueue_PopReturnsEmptyString",
			pushes: nil,
			pops:   1,
			expect: []string{""},
		},
		{
			name:   "SingleItem_ReturnsItem",
			pushes: []string{"a"},
			pops:   1,
			expect: []string{"a"},
		},
		{
			name:   "TwoItems_ReturnsInFIFOOrder",
			pushes: []string{"a", "b"},
			pops:   2,
			expect: []string{"a", "b"},
		},
		{
			name:   "PopBeyondLength_ReturnsEmptyForExtraPops",
			pushes: []string{"a"},
			pops:   3,
			expect: []string{"a", "", ""},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := NewQueue()
			for _, s := range tc.pushes {
				q.Enqueue([]byte(s))
			}
			var got []string
			for i := 0; i < tc.pops; i++ {
				v := q.Dequeue()
				got = append(got, string(v))
			}
			assert.Equal(t, tc.expect, got)
			assert.Equal(t, 0, q.Len()-max(0, len(tc.pushes)-tc.pops))
		})
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
