package integration_test

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	queueapi "corti-kkv/internal/queueapi"
	"corti-kkv/internal/rwclient"
)

// TestWorkerEndToEndOnceMode validates that producing then consuming recreates the original file content exactly.
func TestWorkerEndToEndOnceMode(t *testing.T) {
	ts := httptest.NewServer(queueapi.RegisterRoutes(context.Background()))
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

	if err := client.Consume(ctx, out); err != nil {
		t.Fatalf("consume: %v", err)
	}

	got, _ := os.ReadFile(out)
	if string(got) != string(data) {
		t.Fatalf("output mismatch: got %q want %q", string(got), string(data))
	}
}
