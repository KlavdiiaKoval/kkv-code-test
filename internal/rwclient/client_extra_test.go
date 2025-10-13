package rwclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnqueue_Integration(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
		b, _ := io.ReadAll(r.Body)
		assert.Equal(t, []byte("hello world\n"), b)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	c := New(ts.URL, "q")
	err := c.enqueue(context.Background(), []byte("hello world\n"))
	assert.NoError(t, err)
}

func TestDequeue_Timeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	c := New(ts.URL, "q")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := c.dequeue(ctx)
	assert.Error(t, err)
}

func TestEnqueue_HTTPError(t *testing.T) {
	c := New("http://invalid", "q")
	err := c.enqueue(context.Background(), []byte("fail"))
	assert.Error(t, err)
}

func TestDequeue_HTTPError(t *testing.T) {
	c := New("http://invalid", "q")
	_, err := c.dequeue(context.Background())
	assert.Error(t, err)
}
