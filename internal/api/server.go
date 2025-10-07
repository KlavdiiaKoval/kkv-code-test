package api

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"corti-kkv/internal/queue"
)

type Server struct {
	Manager *queue.QueueManager
}

func NewServer(m *queue.QueueManager) *Server {
	return &Server{Manager: m}
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.handle)
}

func parseQueuePath(p string) (string, bool) {
	if !strings.HasPrefix(p, "/queues/") {
		return "", false
	}
	name := strings.Trim(strings.TrimPrefix(p, "/queues/"), "/")
	if name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	name, ok := parseQueuePath(r.URL.Path)
	if !ok {
		if strings.HasPrefix(r.URL.Path, "/queues/") {
			http.Error(w, "missing or invalid queue name", http.StatusBadRequest)
		} else {
			http.NotFound(w, r)
		}
		return
	}

	switch r.Method {
	case http.MethodHead:
		q := s.Manager.Get(name)
		w.Header().Set("X-Queue-Len", fmt.Sprintf("%d", q.Len()))
		w.WriteHeader(http.StatusOK)
	case http.MethodPost:
		s.handleEnqueue(w, r, name)
	case http.MethodDelete:
		s.handleDequeue(w, name)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request, name string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	if err := r.Body.Close(); err != nil {
		// ignore close error
	}
	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}
	log.Printf("received on queue: %q (%d bytes)", name, len(body))
	q := s.Manager.Get(name)
	q.Enqueue(body)
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleDequeue(w http.ResponseWriter, name string) {
	q := s.Manager.Get(name)
	msg := q.Dequeue()
	if len(msg) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(msg)
}
