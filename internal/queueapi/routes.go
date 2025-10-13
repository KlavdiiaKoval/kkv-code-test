package queueapi

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"
)

func RegisterRoutes(ctx context.Context) http.Handler {
	e := echo.New()
	h := NewHandler()
	e.POST("/queues/:name/messages", h.Enqueue)
	e.DELETE("/queues/:name/messages/head", h.Dequeue)
	return e
}
