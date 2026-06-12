// Command chat is a multi-user chat server demonstrating the websocket
// package. See README.md for running instructions and a guide to the source.
package main

import (
	_ "embed"
	"flag"
	"log"
	"net/http"
)

// home.html is embedded so the example runs from any working directory.
//
//go:embed home.html
var homeHTML []byte

func serveHome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(homeHTML)
}

func main() {
	addr := flag.String("addr", "localhost:8080", "http service address")
	flag.Parse()

	hub := newHub()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", serveHome)
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	log.Printf("listening on http://%s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
