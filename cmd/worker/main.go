package main

import (
	"context"
	"corti-kkv/internal/rwclient"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const dirPerm = 0o755 // rwxr-xr-x

type config struct {
	queueURL      string
	queueName     string
	inPath        string
	outPath       string
	watchDir      string
	watchOutDir   string
	watchInterval time.Duration
}

func main() {
	cfg := parseConfig()
	ctx, cancel := signalContext(context.Background())
	defer cancel()

	if cfg.watchDir == "" {
		if err := os.MkdirAll(cfg.watchOutDir, dirPerm); err != nil {
			log.Fatalf("create watch output dir: %v", err)
		}
	}
	startWatchMode(ctx, cfg)
}

func parseConfig() config {
	var cfg config
	flag.StringVar(&cfg.queueURL, "queue-url", "http://localhost:8080", "queue service base URL")
	flag.StringVar(&cfg.queueName, "queue", "lines", "queue name (base name in watch mode)")
	flag.StringVar(&cfg.inPath, "in", "data/input.txt", "input file path (single-file modes)")
	flag.StringVar(&cfg.outPath, "out", "data/output.txt", "output file path (single-file modes)")
	flag.StringVar(&cfg.watchDir, "watch-dir", "", "if set, watch this directory for new files (disables -in/-out/-mode pipeline)")
	flag.StringVar(&cfg.watchOutDir, "watch-out", "data/out", "output directory for processed files when using -watch-dir")
	flag.DurationVar(&cfg.watchInterval, "watch-interval", 500*time.Millisecond, "poll interval for directory watch mode")
	flag.Parse()
	return cfg
}

func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("signal received: shutting down")
		cancel()
	}()
	return ctx, cancel
}

func processFile(ctx context.Context, cfg config, path string, states map[string]*fileState, mu *sync.Mutex) {
	// 1. Build output/queue names
	qName, outPath := buildOutputPathAndQueueName(cfg, path)

	// 2. Create a new queue client for this file
	client := rwclient.New(cfg.queueURL, qName)

	// 3. Start producing (enqueueing) the file contents to the queue
	producerDone := make(chan error, 1)
	go func() { producerDone <- client.Produce(ctx, path) }()
	<-producerDone // Wait for producer to finish

	// 4. Start consuming (dequeueing) results from the queue to the output file
	err := client.Consume(ctx, outPath)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("watch: error processing %s: %v", path, err)
	} else {
		log.Printf("watch: completed file=%s", path)
	}

	// 5. Mark the file as processed in the state map
	mu.Lock()
	if st := states[path]; st != nil {
		st.processed = true
		st.processing = false
	}
	mu.Unlock()
}

func buildOutputPathAndQueueName(cfg config, path string) (qName, outPath string) {
	base := filepath.Base(path)
	qName = cfg.queueName + "-" + sanitize(base)
	outPath = filepath.Join(cfg.watchOutDir, base)
	log.Printf("watch: processing file=%s queue=%s out=%s", path, qName, outPath)
	return
}

func sanitize(name string) string {
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	s := b.String()
	if s == "" {
		return "file"
	}
	return s
}
