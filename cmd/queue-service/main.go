package main

import (
	"flag"
	"log"
	"net/http"

	api "corti-kkv/internal/api"
	"corti-kkv/internal/queue"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()

	q := queue.NewQueue()
	srv := api.NewServer(q)

	server := &http.Server{
		Addr:    *addr,
		Handler: srv.Handler(),
	}

	log.Printf("queue service listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
