package gosse_test

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/madpac-dev/gosse"
)

// A shopping-list app: each list is a topic, the handler does its own routing
// and authorization, and every change is published as a JSON event.
func Example() {
	b := gosse.NewBroker()
	defer b.Shutdown(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("GET /lists/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		// Check that the user may view this list here, e.g.:
		// if !canView(r, id) { http.Error(w, "forbidden", http.StatusForbidden); return }
		b.Serve(w, r, "list:"+id)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// A client (normally a browser's EventSource) subscribes to list 42.
	resp, err := http.Get(srv.URL + "/lists/42/events")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	// The app saves an item and notifies everyone viewing the list.
	type Item struct {
		Name string `json:"name"`
	}
	if err := b.PublishJSON("list:42", "item-added", Item{Name: "milk"}); err != nil {
		log.Fatal(err)
	}

	// Print the event as it appears on the wire (the ID line is omitted
	// here because IDs are time-based).
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() && sc.Text() != "" {
		if !strings.HasPrefix(sc.Text(), "id:") {
			fmt.Println(sc.Text())
		}
	}
	// Output:
	// event: item-added
	// data: {"name":"milk"}
}

func ExampleBroker_Publish() {
	b := gosse.NewBroker()
	defer b.Shutdown(context.Background())

	// Publish a multi-line, typed event to two topics. Clients subscribed
	// to both receive it once.
	err := b.Publish(context.Background(), gosse.Event{
		Type:  "notice",
		Data:  "line one\nline two",
		Retry: 5 * time.Second,
	}, "list:1", "list:2")
	fmt.Println(err)
	// Output: <nil>
}

func ExampleBroker_Shutdown() {
	b := gosse.NewBroker()
	srv := &http.Server{Addr: ":8080"}

	// Close SSE streams when the server shuts down, so that
	// srv.Shutdown does not wait for them forever.
	srv.RegisterOnShutdown(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	})
}

func ExampleWithReplay() {
	// Keep the last 10,000 events instead of the default 1,000.
	_ = gosse.NewBroker(gosse.WithReplay(gosse.NewMemoryReplayStore(10_000)))

	// Or disable replay entirely.
	_ = gosse.NewBroker(gosse.WithReplay(nil))
}
