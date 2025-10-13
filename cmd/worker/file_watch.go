package main

import (
	"context"
	"sync"
)

type fileState struct {
	size       int64
	stableCnt  int
	processing bool
	processed  bool
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
