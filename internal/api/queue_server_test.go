package api

import (
	"bytes"
	"corti-kkv/internal/queue"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	q := queue.NewQueue()
	s := NewServer(q)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	type step struct {
		method string
		path   string
		body   string
		want   int
		check  string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "EnqueueThenDequeue_ReturnsBody",
			steps: []step{
				{method: http.MethodPost, path: "/queues/a", body: "hello", want: http.StatusAccepted, check: ""},
				{method: http.MethodDelete, path: "/queues/a", body: "", want: http.StatusOK, check: "hello"},
			},
		},
		{
			name: "DequeueOnEmptyQueue_Returns204NoContent",
			steps: []step{
				{method: http.MethodDelete, path: "/queues/empty", body: "", want: http.StatusNoContent, check: ""},
			},
		},
		{
			name: "HeadAfterEnqueue_ReturnsQueueLengthHeader",
			steps: []step{
				{method: http.MethodPost, path: "/queues/h", body: "x", want: http.StatusAccepted, check: ""},
				{method: http.MethodHead, path: "/queues/h", body: "", want: http.StatusOK, check: "HDR-LEN=1"},
			},
		},
		{
			name:  "PostMissingQueueName_Returns400",
			steps: []step{{method: http.MethodPost, path: "/queues/", body: "x", want: http.StatusBadRequest, check: "invalid"}},
		},
		{
			name:  "UnknownPath_Returns404",
			steps: []step{{method: http.MethodGet, path: "/bad", body: "", want: http.StatusNotFound, check: ""}},
		},
		{
			name:  "UnsupportedMethod_Returns405",
			steps: []step{{method: http.MethodPut, path: "/queues/mmm", body: "x", want: http.StatusMethodNotAllowed, check: ""}},
		},
	}

	client := http.DefaultClient

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, st := range tc.steps {
				var req *http.Request
				var err error
				if st.body != "" {
					req, err = http.NewRequest(st.method, ts.URL+st.path, bytes.NewBufferString(st.body))
				} else {
					req, err = http.NewRequest(st.method, ts.URL+st.path, nil)
				}
				assert.NoError(t, err)
				resp, err := client.Do(req)
				assert.NoError(t, err)
				if resp == nil {
					t.FailNow()
				}
				if st.method == http.MethodHead && strings.HasPrefix(st.check, "HDR-LEN=") {
					assert.Equal(t, "1", resp.Header.Get("X-Queue-Len"))
				}
				assert.Equal(t, st.want, resp.StatusCode)
				if st.want == http.StatusOK && st.method != http.MethodHead {
					b, _ := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					if st.check != "" {
						assert.Contains(t, string(b), st.check)
					}
				} else {
					_ = resp.Body.Close()
				}
			}
		})
	}
}
