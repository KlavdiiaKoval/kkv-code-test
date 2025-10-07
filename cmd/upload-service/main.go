package main

import (
	"flag"
	"log"
	"net/http"

	api "corti-kkv/internal/api"
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
	uploadServer := api.NewUploadServer(client, inPath)

	mux := http.NewServeMux()
	mux.Handle("/upload", uploadServer.Handler())

	server := &http.Server{Addr: addr, Handler: mux}
	log.Printf("upload service listening on %s (queue %s at %s)", addr, qName, qURL)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
