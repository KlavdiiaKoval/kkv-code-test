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
	Queue *queue.Queue
}

func NewServer(q *queue.Queue) *Server {
	return &Server{Queue: q}
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
       _, ok := parseQueuePath(r.URL.Path)
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
	       w.Header().Set("X-Queue-Len", fmt.Sprintf("%d", s.Queue.Len()))
	       w.WriteHeader(http.StatusOK)
       case http.MethodPost:
	       s.handleEnqueue(w, r)
       case http.MethodDelete:
	       s.handleDequeue(w)
       default:
	       http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
       }
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
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
	log.Printf("received on queue (%d bytes)", len(body))
	s.Queue.Enqueue(body)
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleDequeue(w http.ResponseWriter) {
	msg := s.Queue.Dequeue()
	if len(msg) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(msg)
}
