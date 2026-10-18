// Command shoppinglist is a minimal real-time shopping list built with gosse.
//
// Run it with
//
//	go run ./examples/shoppinglist
//
// and open http://localhost:8080/lists/groceries in two browser windows.
// Items added in one window appear instantly in the other. Use -addr to
// listen elsewhere.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/madpac-dev/gosse"
)

//go:embed index.html
var indexHTML []byte

// Item is a shopping list entry.
type Item struct {
	Name string `json:"name"`
}

// store is a trivial in-memory list store.
type store struct {
	mu    sync.Mutex
	lists map[string][]Item
}

func (s *store) add(list string, it Item) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists[list] = append(s.lists[list], it)
}

func (s *store) items(list string) []Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Item{}, s.lists[list]...)
}

func main() {
	addr := flag.String("addr", "localhost:8080", "listen address")
	flag.Parse()

	b := gosse.NewBroker()
	db := &store{lists: map[string][]Item{}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /lists/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /lists/{id}/items", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(db.items(r.PathValue("id")))
	})
	mux.HandleFunc("POST /lists/{id}/items", func(w http.ResponseWriter, r *http.Request) {
		var it Item
		if err := json.NewDecoder(r.Body).Decode(&it); err != nil || strings.TrimSpace(it.Name) == "" {
			http.Error(w, "invalid item", http.StatusBadRequest)
			return
		}
		id := r.PathValue("id")
		db.add(id, it)
		if err := b.PublishJSON("list:"+id, "item-added", it); err != nil {
			log.Printf("publish: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("GET /lists/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		// A real app would check here that the user may view this list.
		b.Serve(w, r, "list:"+r.PathValue("id"))
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	srv.RegisterOnShutdown(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	})

	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt)
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	log.Printf("listening on http://%s/lists/groceries", *addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
