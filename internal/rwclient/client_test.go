package rwclient

import (
	"context"
	queueapi "corti-kkv/internal/queueapi"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// newHTTPTestClient creates a Client with a custom URL and optional transport error for testing.
func newHTTPTestClient(url string, transportErr error) *Client {
	httpClient := &http.Client{}
	if transportErr != nil {
		httpClient.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return nil, transportErr
		})
	}
	return &Client{
		QueueURL:   url,
		QueueName:  "q",
		HttpClient: httpClient,
	}
}

// roundTripperFunc allows mocking http.RoundTripper for http.Client.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// waitForFileContent waits until the file at path contains the expected content or times out.
func waitForFileContent(t *testing.T, path string, want []byte, timeout, poll time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		b, err := os.ReadFile(path)
		if err == nil && string(b) == string(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for file %s to have expected content", path)
		}
		time.Sleep(poll)
	}
}

func TestClientProduceConsume(t *testing.T) {
	cases := []struct {
		name         string
		inputContent string
	}{
		{
			name:         "MultiLineFile_TrailingNewline_Preserved",
			inputContent: "line1\nline2\nlast\n",
		},
		{
			name:         "MultiLineFile_NoTrailingNewline_LastLineKept",
			inputContent: "line1\nline2\nlast",
		},
		{
			name:         "SingleLineFile_NoNewline_SentOnce",
			inputContent: "only",
		},
		{
			name:         "EmptyFile_ProducesNoOutput",
			inputContent: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(queueapi.RegisterRoutes(context.Background()))
			defer ts.Close()

			dir := t.TempDir()
			in := filepath.Join(dir, "in.txt")
			out := filepath.Join(dir, "out.txt")
			assert.NoError(t, os.WriteFile(in, []byte(tc.inputContent), 0o644))

			c := New(ts.URL, "test")
			ctx, cancel := context.WithCancel(context.Background())
			consumerDone := make(chan error, 1)
			go func() { consumerDone <- c.Consume(ctx, out) }()

			prodErr := c.Produce(ctx, in)
			assert.NoError(t, prodErr)
			want, _ := os.ReadFile(in)
			waitForFileContent(t, out, want, 1*time.Second, 5*time.Millisecond)
			cancel()
			<-consumerDone
		})
	}
}

func TestClientEnqueue(t *testing.T) {
	cases := []struct {
		name         string
		statusCode   int
		body         string
		transportErr error
		expectErr    bool
	}{
		{
			name:         "Accepted",
			statusCode:   http.StatusAccepted,
			body:         "",
			transportErr: nil,
			expectErr:    false,
		},
		{
			name:         "ServerError",
			statusCode:   http.StatusInternalServerError,
			body:         "boom",
			transportErr: nil,
			expectErr:    true,
		},
		{
			name:         "BadRequest",
			statusCode:   http.StatusBadRequest,
			body:         "bad",
			transportErr: nil,
			expectErr:    true,
		},
		{
			name:         "TransportError",
			statusCode:   0,
			body:         "",
			transportErr: errors.New("dial error"),
			expectErr:    true,
		},
	}

	for _, tc := range cases {
		caze := tc
		t.Run(caze.name, func(t *testing.T) {
			var ts *httptest.Server
			if caze.transportErr == nil {
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, http.MethodPost, r.Method, "method")
					w.WriteHeader(caze.statusCode)
					_, _ = w.Write([]byte(caze.body))
				}))
				defer ts.Close()
			}
			var client *Client
			if ts != nil {
				client = newHTTPTestClient(ts.URL, caze.transportErr)
			} else {
				client = newHTTPTestClient("http://invalid", caze.transportErr)
			}
			err := client.enqueue(context.Background(), []byte("hello\n"))
			if caze.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClientDequeue(t *testing.T) {
	cases := []struct {
		name         string
		responses    []int
		bodies       []string
		transportErr error
		expected     []byte
		wantErr      bool
	}{
		{name: "OK", responses: []int{http.StatusOK}, bodies: []string{"msg"}, expected: []byte("msg")},
		{name: "NoContent", responses: []int{http.StatusNoContent}, bodies: []string{""}, expected: nil},
		{name: "ServerError", responses: []int{http.StatusInternalServerError}, bodies: []string{"fail"}, wantErr: true},
		{name: "TransportErr", transportErr: errors.New("dial"), wantErr: true},
	}

	for _, tc := range cases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			var ts *httptest.Server
			if c.transportErr == nil {
				i := 0
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, http.MethodDelete, r.Method)
					code := c.responses[i]
					body := c.bodies[i]
					i++
					w.WriteHeader(code)
					if body != "" {
						_, _ = w.Write([]byte(body))
					}
				}))
				defer ts.Close()
			}
			client := newHTTPTestClient(func() string {
				if ts != nil {
					return ts.URL
				}
				return "http://invalid"
			}(), c.transportErr)
			val, err := client.dequeue(context.Background())
			if c.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, string(c.expected), string(val))
			}
		})
	}
}

func TestClientProduce(t *testing.T) {
	cases := []struct {
		name            string
		fileData        string
		setupCtx        func() (context.Context, context.CancelFunc)
		wantErrContains string
	}{
		{
			name:            "FileNotFound",
			fileData:        "",
			setupCtx:        func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			wantErrContains: "no such file",
		},
		{
			name:     "CancelMidway",
			fileData: strings.Repeat("line\n", 2000),
			setupCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				go func() { time.Sleep(2 * time.Millisecond); cancel() }()
				return ctx, cancel
			},
			wantErrContains: context.Canceled.Error(),
		},
		{
			name:            "Success",
			fileData:        "a\nb\nlast",
			setupCtx:        func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			wantErrContains: "",
		},
	}

	for _, tc := range cases {
		caze := tc
		t.Run(caze.name, func(t *testing.T) {
			// test server always 202 Accept
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) }))
			defer ts.Close()
			client := New(ts.URL, "q")
			ctx, cancel := caze.setupCtx()
			defer cancel()

			path := filepath.Join(t.TempDir(), "in.txt")
			if caze.name != "FileNotFound" {
				if err := os.WriteFile(path, []byte(caze.fileData), 0644); err != nil {
					assert.Failf(t, "write", "write file: %v", err)
				}
			}
			err := client.Produce(ctx, func() string {
				if caze.name == "FileNotFound" {
					return filepath.Join(t.TempDir(), "missing.txt")
				}
				return path
			}())
			if caze.wantErrContains != "" {
				if assert.Error(t, err) {
					assert.Contains(t, err.Error(), caze.wantErrContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClientConsume(t *testing.T) {
	cases := []struct {
		name            string
		setupServer     func(seq *int) *httptest.Server
		setupCtx        func() (context.Context, context.CancelFunc)
		prepareOutPath  func(base string) (string, bool) // returns path and whether we expect initial create error
		expectContent   string
		wantErrContains string
	}{
		{
			name: "CreateFileError",
			setupServer: func(seq *int) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
			},
			setupCtx: func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			prepareOutPath: func(base string) (string, bool) {
				d := filepath.Join(base, "dir")
				os.MkdirAll(d, 0755)
				return d, true
			},
			wantErrContains: "is a directory",
		},
		{
			name: "ImmediateCancel",
			setupServer: func(seq *int) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			prepareOutPath: func(base string) (string, bool) { return filepath.Join(base, "out.txt"), false },
			expectContent:  "",
		},
		{
			name: "ReceiveMessagesThenCancel",
			setupServer: func(seq *int) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodDelete {
						t.Fatalf("expected DELETE got %s", r.Method)
					}
					if *seq < 3 {
						w.WriteHeader(http.StatusOK)
						fmt.Fprintf(w, "m%d", *seq)
					} else {
						w.WriteHeader(http.StatusNoContent)
					}
					*seq++
				}))
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				go func() { time.Sleep(15 * time.Millisecond); cancel() }()
				return ctx, cancel
			},
			prepareOutPath: func(base string) (string, bool) { return filepath.Join(base, "out.txt"), false },
			expectContent:  "m0m1m2",
		},
		{
			name: "ServerErrorsIgnored",
			setupServer: func(seq *int) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					io.WriteString(w, "err")
				}))
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				go func() { time.Sleep(10 * time.Millisecond); cancel() }()
				return ctx, cancel
			},
			prepareOutPath: func(base string) (string, bool) { return filepath.Join(base, "out.txt"), false },
			expectContent:  "",
		},
	}

	for _, tc := range cases {
		caze := tc
		t.Run(caze.name, func(t *testing.T) {
			seq := 0
			ts := caze.setupServer(&seq)
			defer ts.Close()
			client := New(ts.URL, "q")
			ctx, cancel := caze.setupCtx()
			defer cancel()
			base := t.TempDir()
			outPath, expectCreateErr := caze.prepareOutPath(base)
			err := client.Consume(ctx, outPath)
			if expectCreateErr {
				if assert.Error(t, err) {
					assert.Contains(t, err.Error(), caze.wantErrContains)
				}
				return
			}
			assert.NoError(t, err)
			b, _ := os.ReadFile(outPath)
			assert.Equal(t, caze.expectContent, string(b))
		})
	}
}
