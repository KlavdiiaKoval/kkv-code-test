package integration_test

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "corti-kkv/internal/api"
	"corti-kkv/internal/queue"
	"corti-kkv/internal/rwclient"
)

// TestWorkerEndToEndOnceMode validates that producing then consuming recreates the original file content exactly.
func TestWorkerEndToEndOnceMode(t *testing.T) {
	m := queue.NewQueueManager()
	srv := api.NewServer(m)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	dir := t.TempDir()
	in := filepath.Join(dir, "input.txt")
	out := filepath.Join(dir, "output.txt")
	data := []byte("line1\nline2\nlast-no-nl")
	if err := os.WriteFile(in, data, 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}

	client := rwclient.New(ts.URL, "lines")
	ctx := context.Background()
	if err := client.Produce(ctx, in); err != nil {
		t.Fatalf("produce: %v", err)
	}

	end := time.Now().Add(2 * time.Second)
	f, err := os.Create(out)
	if err != nil {
		t.Fatalf("create out: %v", err)
	}
	defer f.Close()
	for time.Now().Before(end) {
		b, err := client.Dequeue(ctx)
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if len(b) == 0 {
			ln, _ := client.QueueLength(ctx)
			if ln == 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if _, err := f.Write(b); err != nil {
			t.Fatalf("write out: %v", err)
		}
	}

	got, _ := os.ReadFile(out)
	if string(got) != string(data) {
		t.Fatalf("output mismatch: got %q want %q", string(got), string(data))
	}
}
