package queueapi

import (
	"corti-kkv/internal/queue"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"
)

type EnqueueRequest struct {
	Message string `json:"message"`
}

type Handler struct {
	QueueManager *queue.QueueManager
}

func NewHandler() *Handler {
	return &Handler{QueueManager: queue.NewQueueManager()}
}

func (h *Handler) Enqueue(c echo.Context) error {
	queueName := c.Param("name")
	if queueName == "" {
		return c.String(http.StatusBadRequest, "invalid queue name")
	}

	switch c.Request().Header.Get("Content-Type") {
	case "application/octet-stream":
		return h.enqueueOctetStream(c, queueName)
	default:
		return h.enqueueJSON(c, queueName)
	}
}

func (h *Handler) enqueueOctetStream(c echo.Context, queueName string) error {
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}
	if len(body) == 0 {
		return c.String(http.StatusBadRequest, "message is required")
	}
	h.QueueManager.EnqueueWithName(queueName, body)
	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) enqueueJSON(c echo.Context, queueName string) error {
	var req EnqueueRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}
	if req.Message == "" {
		return c.String(http.StatusBadRequest, "message is required")
	}
	h.QueueManager.EnqueueWithName(queueName, []byte(req.Message))
	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) Dequeue(c echo.Context) error {
	queueName := c.Param("name")
	if queueName == "" {
		return c.String(http.StatusBadRequest, "invalid queue name")
	}
	msg := h.QueueManager.DequeueWithName(queueName)
	if len(msg) == 0 {
		return c.NoContent(http.StatusNoContent)
	}
	return c.Blob(http.StatusOK, "application/octet-stream", msg)
}
