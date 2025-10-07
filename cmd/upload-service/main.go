package main

import (
	"context"
	"flag"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"corti-kkv/internal/rwclient"
)

func main() {
	var (
		addr    string
		qURL    string
		qName   string
		inPath  string
		outPath string
	)
	flag.StringVar(&addr, "addr", ":8081", "address to listen on")
	flag.StringVar(&qURL, "queue-url", "http://localhost:8080", "queue service base URL")
	flag.StringVar(&qName, "queue", "lines", "queue name")
	flag.StringVar(&inPath, "in", "/data/input.txt", "path to input file")
	flag.StringVar(&outPath, "out", "/data/output.txt", "path to output file")
	flag.Parse()

	client := rwclient.New(qURL, qName)

	mux := http.NewServeMux()
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ct := r.Header.Get("Content-Type")
		mediatype, _, err := mime.ParseMediaType(ct)
		if err != nil {
			mediatype = ct
		}

		var reader io.Reader
		switch {
		case strings.HasPrefix(mediatype, "multipart/"):
			if err := r.ParseMultipartForm(32 << 20); err != nil { // 32MB
				http.Error(w, "invalid multipart form", http.StatusBadRequest)
				return
			}
			file, header, err := r.FormFile("file")
			if err != nil {
				http.Error(w, "missing file field", http.StatusBadRequest)
				return
			}
			defer file.Close()
			// save to inPath (or derive from filename in /data)
			dest := inPath
			if dest == "" {
				dest = filepath.Join("/data", filepath.Base(header.Filename))
			}
			if err := saveToFile(dest, file); err != nil {
				log.Printf("save error: %v", err)
				http.Error(w, "failed to save file", http.StatusInternalServerError)
				return
			}
			if err := client.Produce(context.Background(), dest); err != nil {
				log.Printf("produce error: %v", err)
				http.Error(w, "failed to enqueue", http.StatusInternalServerError)
				return
			}
			// kick off brief drain loop to help consumer flush
			// (Maybe there is a better way to do this?)
			deadline := time.Now().Add(200 * time.Millisecond)
			for time.Now().Before(deadline) {
				n, err := client.QueueLength(context.Background())
				if err == nil && n == 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			w.WriteHeader(http.StatusAccepted)
			return
		default:
			reader = r.Body
			defer r.Body.Close()
		}

		if reader == nil {
			http.Error(w, "no input provided", http.StatusBadRequest)
			return
		}
		if err := saveToFile(inPath, reader); err != nil {
			log.Printf("save error: %v", err)
			http.Error(w, "failed to save file", http.StatusInternalServerError)
			return
		}
		if err := client.Produce(context.Background(), inPath); err != nil {
			log.Printf("produce error: %v", err)
			http.Error(w, "failed to enqueue", http.StatusInternalServerError)
			return
		}
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) {
			n, err := client.QueueLength(context.Background())
			if err == nil && n == 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		w.WriteHeader(http.StatusAccepted)
	})

	server := &http.Server{Addr: addr, Handler: mux}
	log.Printf("upload service listening on %s (queue %s at %s)", addr, qName, qURL)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func saveToFile(path string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return err
	}
	return nil
}
