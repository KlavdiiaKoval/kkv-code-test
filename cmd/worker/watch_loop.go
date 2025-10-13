package main

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"time"
)

func startWatchMode(ctx context.Context, cfg config) {
	if err := os.MkdirAll(cfg.watchOutDir, dirPerm); err != nil {
		log.Fatalf("create watch output dir: %v", err)
	}
	log.Printf("worker start (watch mode): dir=%s outDir=%s baseQueue=%s interval=%s", cfg.watchDir, cfg.watchOutDir, cfg.queueName, cfg.watchInterval)
	if err := runWatchMode(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("watch mode error: %v", err)
		os.Exit(1)
	}
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

func processWatchDir(ctx context.Context, cfg config, states map[string]*fileState, mu *sync.Mutex, wg *sync.WaitGroup) {
	entries := readWatchDirEntries(cfg.watchDir)

	for _, entry := range entries {
		processWatchEntry(ctx, cfg, entry, states, mu, wg)
	}
}

func StringStartsWithDot(s string) bool {
	return len(s) > 0 && s[0] == '.'
}

func processWatchEntry(ctx context.Context, cfg config, entry os.DirEntry, states map[string]*fileState, mu *sync.Mutex, wg *sync.WaitGroup) {
	if entry.IsDir() {
		return
	}
	name := entry.Name()
	if StringStartsWithDot(name) {
		return
	}
	full := cfg.watchDir + string(os.PathSeparator) + name

	info, err := entry.Info()
	if err != nil {
		return
	}
	handleFileState(ctx, cfg, full, info.Size(), states, mu, wg) // This will call processFile when the file is stable
}

func readWatchDirEntries(dir string) []os.DirEntry {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("watch: read dir error: %v", err)
		return nil
	}
	return entries
}
