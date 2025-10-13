package main

import (
	"context"
	"corti-kkv/internal/queueapi"
	"flag"
	"log"
	"net/http"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()

	h := queueapi.RegisterRoutes(context.Background())

	server := &http.Server{
		Addr:    *addr,
		Handler: h,
	}
	log.Printf("queue service listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
