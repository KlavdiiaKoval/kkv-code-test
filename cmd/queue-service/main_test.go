package main

import (
	"context"
	"corti-kkv/internal/queueapi"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestQueueServiceMain_RegisterRoutes(t *testing.T) {
	h := queueapi.RegisterRoutes(context.Background())
	ts := httptest.NewServer(h)
	defer ts.Close()

	cases := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{"enqueue missing queue", "POST", "/queues//messages", 400},
		{"dequeue missing queue", "DELETE", "/queues//messages/head", 400},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, _ := http.NewRequest(c.method, ts.URL+c.path, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request error: %v", err)
			}
			if resp.StatusCode != c.want {
				t.Errorf("%s %s: got %d, want %d", c.method, c.path, resp.StatusCode, c.want)
			}
		})
	}
}
