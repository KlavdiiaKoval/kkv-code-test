package api

import (
	"context"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// UploadServer handles the /upload endpoint and enqueues uploaded content
// into the configured queue using the rwclient.
type UploadServer struct {
	Client    Producer
	InputPath string
}

// Producer abstracts the minimal queue client API needed by UploadServer.
// Implemented by rwclient.Client.
type Producer interface {
	Produce(ctx context.Context, inputPath string) error
	QueueLength(ctx context.Context) (int, error)
}

func NewUploadServer(c Producer, inPath string) *UploadServer {
	return &UploadServer{Client: c, InputPath: inPath}
}

func (s *UploadServer) Handler() http.Handler {
	return http.HandlerFunc(s.handle)
}

func (s *UploadServer) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/upload" {
		http.NotFound(w, r)
		return
	}
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
		// save to InputPath (or derive from filename in /data)
		dest := s.InputPath
		if dest == "" {
			dest = filepath.Join("/data", filepath.Base(header.Filename))
		}
		if err := saveToFile(dest, file); err != nil {
			log.Printf("save error: %v", err)
			http.Error(w, "failed to save file", http.StatusInternalServerError)
			return
		}
		if err := s.Client.Produce(context.Background(), dest); err != nil {
			log.Printf("produce error: %v", err)
			http.Error(w, "failed to enqueue", http.StatusInternalServerError)
			return
		}
		// brief drain loop to help consumer flush
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) {
			n, err := s.Client.QueueLength(context.Background())
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
	if err := saveToFile(s.InputPath, reader); err != nil {
		log.Printf("save error: %v", err)
		http.Error(w, "failed to save file", http.StatusInternalServerError)
		return
	}
	if err := s.Client.Produce(context.Background(), s.InputPath); err != nil {
		log.Printf("produce error: %v", err)
		http.Error(w, "failed to enqueue", http.StatusInternalServerError)
		return
	}
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		n, err := s.Client.QueueLength(context.Background())
		if err == nil && n == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	w.WriteHeader(http.StatusAccepted)
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
