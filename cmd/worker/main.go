package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"corti-kkv/internal/rwclient"
)

// Simple worker that reads an input file, enqueues each line, then drains the queue to an output file.
// When mode=once (default) it exits after the queue is drained. If mode=stream it will keep consuming.
// config holds runtime configuration parsed from flags.
type config struct {
	queueURL      string
	queueName     string
	inPath        string
	outPath       string
	mode          string // once|stream for single-file modes
	watchDir      string // if set enables directory watch mode (multi-file once copy)
	watchOutDir   string // output directory for watched files (mirrors filenames)
	watchInterval time.Duration
}

func main() {
	cfg := parseConfig()

	ctx, cancel := signalContext(context.Background())
	defer cancel()

	if cfg.watchDir != "" {
		if err := os.MkdirAll(cfg.watchOutDir, 0o755); err != nil {
			log.Fatalf("create watch output dir: %v", err)
		}
		log.Printf("worker start (watch mode): dir=%s outDir=%s baseQueue=%s interval=%s", cfg.watchDir, cfg.watchOutDir, cfg.queueName, cfg.watchInterval)
		if err := runWatchMode(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("watch mode error: %v", err)
			os.Exit(1)
		}
		return
	}

	ensureOutputDir(cfg.outPath)

	// Optional: show input file size if it exists
	var inSize int64
	if st, err := os.Stat(cfg.inPath); err == nil {
		inSize = st.Size()
	}
	log.Printf("worker start: queueURL=%s queue=%s mode=%s in=%s(size=%d) out=%s", cfg.queueURL, cfg.queueName, cfg.mode, cfg.inPath, inSize, cfg.outPath)

	client := rwclient.New(cfg.queueURL, cfg.queueName)
	prodErr, consErr := runPipeline(ctx, client, cfg)

	hasErr := false
	if prodErr != nil && !errors.Is(prodErr, context.Canceled) {
		hasErr = true
		log.Printf("producer error: %v", prodErr)
	}
	if consErr != nil && !errors.Is(consErr, context.Canceled) {
		hasErr = true
		log.Printf("consumer error: %v", consErr)
	}
	if hasErr {
		os.Exit(1)
	}
	log.Printf("worker finished successfully")
}

func parseConfig() config {
	var cfg config
	flag.StringVar(&cfg.queueURL, "queue-url", "http://localhost:8080", "queue service base URL")
	flag.StringVar(&cfg.queueName, "queue", "lines", "queue name (base name in watch mode)")
	flag.StringVar(&cfg.inPath, "in", "data/input.txt", "input file path (single-file modes)")
	flag.StringVar(&cfg.outPath, "out", "data/output.txt", "output file path (single-file modes)")
	flag.StringVar(&cfg.mode, "mode", "once", "single-file mode: once|stream (ignored when -watch-dir set)")
	flag.StringVar(&cfg.watchDir, "watch-dir", "", "if set, watch this directory for new files (disables -in/-out/-mode pipeline)")
	flag.StringVar(&cfg.watchOutDir, "watch-out", "data/out", "output directory for processed files when using -watch-dir")
	flag.DurationVar(&cfg.watchInterval, "watch-interval", 500*time.Millisecond, "poll interval for directory watch mode")
	flag.Parse()
	return cfg
}

func ensureOutputDir(path string) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}
}

// signalContext returns a context cancelled on SIGINT or SIGTERM.
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

func runPipeline(ctx context.Context, client *rwclient.Client, cfg config) (prodErr, consErr error) {
	var wg sync.WaitGroup
	producerDone := make(chan error, 1)
	consumerDone := make(chan error, 1)

	// Producer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if cfg.mode == "stream" {
			producerDone <- produceStream(ctx, client, cfg.inPath)
			return
		}
		producerDone <- client.Produce(ctx, cfg.inPath)
	}()

	// Consumer goroutine: if once mode, we wrap client.Consume with cancellation after drain
	wg.Add(1)
	go func() {
		defer wg.Done()
		if cfg.mode == "once" {
			consumerDone <- consumeOnce(ctx, client, cfg.outPath, producerDone)
			return
		}
		consumerDone <- client.Consume(ctx, cfg.outPath)
	}()

	wg.Wait()
	prodErr = <-producerDone
	consErr = <-consumerDone
	return
}

// runWatchMode watches a directory for new regular files and, once a file size stabilizes,
// copies it (once mode) through an isolated queue (queueName = baseQueue + "-" + sanitizedFilename)
// into an output file with the same base name inside watchOutDir. Each file is processed only once.
func runWatchMode(ctx context.Context, cfg config) error {
	type fileState struct {
		size       int64
		stableCnt  int
		processing bool
		processed  bool
	}
	states := make(map[string]*fileState)
	var mu sync.Mutex
	wg := &sync.WaitGroup{}

	ticker := time.NewTicker(cfg.watchInterval)
	defer ticker.Stop()

	processFile := func(path string) {
		base := filepath.Base(path)
		qName := cfg.queueName + "-" + sanitize(base)
		outPath := filepath.Join(cfg.watchOutDir, base)
		log.Printf("watch: processing file=%s queue=%s out=%s", path, qName, outPath)
		client := rwclient.New(cfg.queueURL, qName)
		// build a producerDone channel then reuse consumeOnce
		producerDone := make(chan error, 1)
		go func() { producerDone <- client.Produce(ctx, path) }()
		if err := consumeOnce(ctx, client, outPath, producerDone); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("watch: error processing %s: %v", path, err)
		} else {
			log.Printf("watch: completed file=%s", path)
		}
		mu.Lock()
		if st := states[path]; st != nil {
			st.processed = true
			st.processing = false
		}
		mu.Unlock()
	}

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			entries, err := os.ReadDir(cfg.watchDir)
			if err != nil {
				log.Printf("watch: read dir error: %v", err)
				continue
			}
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				name := e.Name()
				if strings.HasPrefix(name, ".") { // skip hidden / dotfiles
					continue
				}
				full := filepath.Join(cfg.watchDir, name)
				info, err := e.Info()
				if err != nil {
					continue
				}
				// track state
				mu.Lock()
				st := states[full]
				if st == nil {
					st = &fileState{size: info.Size()}
					states[full] = st
					mu.Unlock()
					continue
				}
				if st.processed || st.processing { // already done or in-progress
					mu.Unlock()
					continue
				}
				if info.Size() == st.size {
					st.stableCnt++
				} else {
					st.size = info.Size()
					st.stableCnt = 0
				}
				if st.stableCnt >= 1 { // size stable across two scans
					st.processing = true
					wg.Add(1)
					go func(p string) {
						defer wg.Done()
						processFile(p)
					}(full)
				}
				mu.Unlock()
			}
		}
	}
}

// sanitize converts a filename into a queue-safe suffix.
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

// consumeOnce drains the queue to the output file then returns when the producer is finished and the queue is empty.
func consumeOnce(ctx context.Context, c *rwclient.Client, outPath string, producerDone <-chan error) error {
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	log.Printf("consumer: writing to %s", outPath)

	idle := 10 * time.Millisecond
	producerFinished := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		b, err := c.Dequeue(ctx)
		if err != nil {
			return err
		}
		if len(b) > 0 {
			if _, err := f.Write(b); err != nil {
				return err
			}
			continue
		}
		// empty
		if !producerFinished {
			select {
			case <-producerDone:
				producerFinished = true
			default:
			}
		}
		if producerFinished {
			ln, err := c.QueueLength(ctx)
			if err == nil && ln == 0 {
				log.Printf("consumer: completed once mode")
				return nil
			}
		}
		time.Sleep(idle)
	}
}

// produceStream tails the input file: it starts from current size (creating if missing), then watches for appended data.
// Each full line (ending with '\n') is enqueued immediately; a partial final line is held until newline is written or context ends.
// It never returns unless context is canceled or a fatal error occurs.
func produceStream(ctx context.Context, c *rwclient.Client, path string) error {
	const pollInterval = 300 * time.Millisecond
	var offset int64
	var partial []byte

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		f, err := os.Open(path) // open each loop to catch rotations/truncations
		if os.IsNotExist(err) {
			// File missing: brief sleep then retry
			time.Sleep(pollInterval)
			continue
		}
		if err != nil {
			return err
		}
		// Get current size and adjust offset if truncated
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		if st.Size() < offset {
			// truncated
			offset = 0
			partial = nil
		}
		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return err
		}

		buf := make([]byte, 32*1024)
		readSomething := false
		for {
			select {
			case <-ctx.Done():
				f.Close()
				return ctx.Err()
			default:
			}
			n, err := f.Read(buf)
			if n > 0 {
				readSomething = true
				offset += int64(n)
				chunk := buf[:n]
				// prepend partial
				if len(partial) > 0 {
					chunk = append(partial, chunk...)
					partial = nil
				}
				// split on newlines
				start := 0
				for i, b := range chunk {
					if b == '\n' {
						line := chunk[start : i+1]
						if e := c.Enqueue(ctx, line); e != nil {
							f.Close()
							return e
						}
						start = i + 1
					}
				}
				if start < len(chunk) { // remaining partial fragment
					partial = append(partial, chunk[start:]...)
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				f.Close()
				return err
			}
		}
		f.Close()
		if !readSomething {
			// nothing new; sleep then poll again
			time.Sleep(pollInterval)
		}
	}
}
