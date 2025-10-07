package rwclient

import (
	"net/http"
	"os"
	"testing"
	"time"
)

type errorRoundTripper struct{ err error }

func (e errorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) { return nil, e.err }

func newHTTPTestClient(tsURL string, rtErr error) *Client {
	c := New(tsURL, "q")
	if rtErr != nil {
		c.HttpClient = &http.Client{Transport: errorRoundTripper{err: rtErr}}
	}
	return c
}

func waitForFileContent(t *testing.T, path string, want []byte, timeout time.Duration, interval time.Duration) {
	t.Helper()
	if interval <= 0 {
		interval = 5 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got, err := os.ReadFile(path)
		if err == nil && string(got) == string(want) {
			return
		}
		time.Sleep(interval)
	}
	got, _ := os.ReadFile(path)
	if string(got) != string(want) {
		t.Fatalf("content mismatch after timeout: got %q want %q", string(got), string(want))
	}
}
