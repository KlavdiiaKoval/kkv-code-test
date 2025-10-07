package rwclient

import (
	"context"
	api "corti-kkv/internal/api"
	"corti-kkv/internal/queue"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
			m := queue.NewQueueManager()
			s := api.NewServer(m)
			ts := httptest.NewServer(s.Handler())
			defer ts.Close()

			dir := t.TempDir()
			in := filepath.Join(dir, "in.txt")
			out := filepath.Join(dir, "out.txt")
			assert.NoError(t, os.WriteFile(in, []byte(tc.inputContent), 0644))

			c := New(ts.URL, "test")
			ctx, cancel := context.WithCancel(context.Background())
			consumerDone := make(chan error, 1)
			go func() { consumerDone <- c.Consume(ctx, out) }()

			prodErr := c.Produce(ctx, in)
			assert.NoError(t, prodErr)

			want, _ := os.ReadFile(in)
			// poll until output content equals want or timeout
			for i := 0; i < 200; i++ { // up to ~1s
				got, _ := os.ReadFile(out)
				if string(got) == string(want) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			got, _ := os.ReadFile(out)
			assert.Equal(t, string(want), string(got))
			cancel()
			<-consumerDone
		})
	}
}
