package queueapi

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

func TestHandler_Enqueue(t *testing.T) {
	cases := []struct {
		name        string
		headers     map[string]string
		body        io.Reader
		wantStatus  int
		wantInQueue []byte
	}{
		{
			name:        "octet-stream valid",
			headers:     map[string]string{"Content-Type": "application/octet-stream"},
			body:        bytes.NewBuffer([]byte("abc")),
			wantStatus:  http.StatusAccepted,
			wantInQueue: []byte("abc"),
		},
		{
			name:       "octet-stream empty",
			headers:    map[string]string{"Content-Type": "application/octet-stream"},
			body:       bytes.NewBuffer([]byte{}),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:        "json valid",
			headers:     map[string]string{"Content-Type": "application/json"},
			body:        bytes.NewBufferString(`{"message":"hello"}`),
			wantStatus:  http.StatusAccepted,
			wantInQueue: []byte("hello"),
		},
		{
			name:       "json missing message",
			headers:    map[string]string{"Content-Type": "application/json"},
			body:       bytes.NewBufferString(`{"message":""}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing queue name",
			headers:    map[string]string{"Content-Type": "application/json"},
			body:       bytes.NewBufferString(`{"message":"x"}`),
			wantStatus: http.StatusBadRequest,
		},
	}
	e := echo.New()
	h := NewHandler()
	e.POST("/queues/:name/messages", h.Enqueue)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			queueName := "q"
			if c.name == "missing queue name" {
				queueName = ""
			}
			req := httptest.NewRequest(http.MethodPost, "/queues/"+queueName+"/messages", c.body)
			for k, v := range c.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			ctx := e.NewContext(req, rec)
			ctx.SetParamNames("name")
			ctx.SetParamValues(queueName)
			err := h.Enqueue(ctx)
			if err != nil {
				e, ok := err.(*echo.HTTPError)
				if ok && rec.Code == 0 {
					rec.WriteHeader(e.Code)
				}
			}
			assert.Equal(t, c.wantStatus, rec.Code, "status code")
			if c.wantInQueue != nil && rec.Code == http.StatusAccepted {
				got := h.QueueManager.DequeueWithName(queueName)
				assert.Equal(t, c.wantInQueue, got, "queue value")
			}
		})
	}
}

func TestHandler_Dequeue(t *testing.T) {
	e := echo.New()
	h := NewHandler()
	e.DELETE("/queues/:name/messages/head", h.Dequeue)
	queueName := "q"
	h.QueueManager.EnqueueWithName(queueName, []byte("abc"))

	cases := []struct {
		name       string
		queue      string
		wantStatus int
		wantBody   []byte
	}{
		{"valid", queueName, http.StatusOK, []byte("abc")},
		{"empty", queueName, http.StatusNoContent, nil},
		{"missing queue name", "", http.StatusBadRequest, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodDelete, "/queues/"+c.queue+"/messages/head", nil)
			rec := httptest.NewRecorder()
			ctx := e.NewContext(req, rec)
			ctx.SetParamNames("name")
			ctx.SetParamValues(c.queue)
			err := h.Dequeue(ctx)
			if err != nil {
				e, ok := err.(*echo.HTTPError)
				if ok && rec.Code == 0 {
					rec.WriteHeader(e.Code)
				}
			}
			assert.Equal(t, c.wantStatus, rec.Code, "status code")
			if c.wantBody != nil {
				assert.Equal(t, string(c.wantBody), rec.Body.String(), "body")
			}
		})
	}
}
