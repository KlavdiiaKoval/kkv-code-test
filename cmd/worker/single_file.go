package main

import (
	"context"
	"log"
	"os"
)

func startSingleFileMode(ctx context.Context, cfg config) {
	if err := os.MkdirAll(getOutputDir(cfg.outPath), 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}
	var inSize int64
	if st, err := os.Stat(cfg.inPath); err == nil {
		inSize = st.Size()
	}
	log.Printf("worker start: queueURL=%s queue=%s mode=%s in=%s(size=%d) out=%s", cfg.queueURL, cfg.queueName, cfg.mode, cfg.inPath, inSize, cfg.outPath)

	processFile(ctx, cfg, cfg.inPath, nil, nil)

	log.Printf("worker finished successfully")
}

func getOutputDir(path string) string {
	i := len(path) - 1
	for ; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
