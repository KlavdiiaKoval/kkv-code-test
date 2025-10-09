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

// Types
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

type fileState struct {
	size       int64
	stableCnt  int
	processing bool
	processed  bool
}

func main() {
	cfg := parseConfig()
	ctx, cancel := signalContext(context.Background())
	defer cancel()

	if cfg.watchDir != "" {
		startWatchMode(ctx, cfg)
		return
	}

	startSingleFileMode(ctx, cfg)
}

func startWatchMode(ctx context.Context, cfg config) {
	if err := os.MkdirAll(cfg.watchOutDir, 0o755); err != nil {
		log.Fatalf("create watch output dir: %v", err)
	}
	log.Printf("worker start (watch mode): dir=%s outDir=%s baseQueue=%s interval=%s", cfg.watchDir, cfg.watchOutDir, cfg.queueName, cfg.watchInterval)
	if err := runWatchMode(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("watch mode error: %v", err)
		os.Exit(1)
	}
}

func startSingleFileMode(ctx context.Context, cfg config) {
	ensureOutputDir(cfg.outPath)
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

func runWatchMode(ctx context.Context, cfg config) error {
	states := make(map[string]*fileState)
	var mu sync.Mutex
	wg := &sync.WaitGroup{}

	ticker := time.NewTicker(cfg.watchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			processWatchDir(ctx, cfg, states, &mu, wg)
		}
	}
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

func processWatchDir(ctx context.Context, cfg config, states map[string]*fileState, mu *sync.Mutex, wg *sync.WaitGroup) {
	entries := readWatchDirEntries(cfg.watchDir)
	for _, entry := range entries {
		processWatchEntry(ctx, cfg, entry, states, mu, wg)
	}
}

func readWatchDirEntries(dir string) []os.DirEntry {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("watch: read dir error: %v", err)
		return nil
	}
	return entries
}

func processWatchEntry(ctx context.Context, cfg config, entry os.DirEntry, states map[string]*fileState, mu *sync.Mutex, wg *sync.WaitGroup) {
	if entry.IsDir() {
		return
	}
	name := entry.Name()
	if strings.HasPrefix(name, ".") {
		return
	}
	full := filepath.Join(cfg.watchDir, name)
	info, err := entry.Info()
	if err != nil {
		return
	}
	handleFileState(ctx, cfg, full, info.Size(), states, mu, wg)
}

func handleFileState(ctx context.Context, cfg config, full string, size int64, states map[string]*fileState, mu *sync.Mutex, wg *sync.WaitGroup) {
	mu.Lock()
	defer mu.Unlock()
	st := states[full]
	if st == nil {
		states[full] = &fileState{size: size}
		return
	}
	if st.processed || st.processing {
		return
	}
	updateFileState(st, size)
	if isFileStable(st) {
		st.processing = true
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			processFile(ctx, cfg, p, states, mu)
		}(full)
	}
}

func updateFileState(st *fileState, size int64) {
	if size == st.size {
		st.stableCnt++
	} else {
		st.size = size
		st.stableCnt = 0
	}
}

func isFileStable(st *fileState) bool {
	return st.stableCnt >= 1
}

func processFile(ctx context.Context, cfg config, path string, states map[string]*fileState, mu *sync.Mutex) {
	base := filepath.Base(path)
	qName := cfg.queueName + "-" + sanitize(base)
	outPath := filepath.Join(cfg.watchOutDir, base)
	log.Printf("watch: processing file=%s queue=%s out=%s", path, qName, outPath)
	client := rwclient.New(cfg.queueURL, qName)
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
		if err := checkContext(ctx); err != nil {
			return err
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
			producerFinished = waitForProducer(producerDone)
		}
		if producerFinished && queueIsEmpty(ctx, c) {
			log.Printf("consumer: completed once mode")
			return nil
		}
		time.Sleep(idle)
	}
}

// checkContext returns ctx.Err() if context is done, otherwise nil
func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// waitForProducer returns true if the producer is finished
func waitForProducer(producerDone <-chan error) bool {
	select {
	case <-producerDone:
		return true
	default:
		return false
	}
}

// queueIsEmpty returns true if the queue is empty, false otherwise
func queueIsEmpty(ctx context.Context, c *rwclient.Client) bool {
	ln, err := c.QueueLength(ctx)
	return err == nil && ln == 0
}

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
