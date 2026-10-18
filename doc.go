// Package gosse adds Server-Sent Events (SSE) to any net/http server.
//
// It implements the server side of the WHATWG "Server-sent events"
// specification (https://html.spec.whatwg.org/multipage/server-sent-events.html)
// and is designed so that simple applications need no configuration at all:
// reconnection, catching up on missed events and keep-alive heartbeats work
// out of the box with a plain browser EventSource.
//
// # Quick start
//
// Create one [Broker] for your application, call [Broker.Serve] from your own
// handler (after your own routing and authorization), and publish events to
// topics:
//
//	b := gosse.NewBroker()
//
//	mux.HandleFunc("GET /lists/{id}/events", func(w http.ResponseWriter, r *http.Request) {
//		id := r.PathValue("id")
//		if !canView(r, id) {
//			http.Error(w, "forbidden", http.StatusForbidden)
//			return
//		}
//		b.Serve(w, r, "list:"+id)
//	})
//
//	// Somewhere else, e.g. after saving an item:
//	b.PublishJSON("list:"+id, "item-added", item)
//
// In the browser:
//
//	const es = new EventSource("/lists/42/events");
//	es.addEventListener("item-added", (e) => console.log(JSON.parse(e.data)));
//
// # Topics
//
// Topics are arbitrary strings chosen by the application. A subscriber may
// listen to several topics; an event published to several topics is delivered
// to each subscriber at most once.
//
// # Event IDs and replay
//
// The broker assigns every event a unique, increasing numeric ID. Browsers
// remember the last ID they received and send it back in the Last-Event-ID
// header when they reconnect. By default the broker keeps the most recent
// [DefaultReplayCapacity] events in memory and resends the ones the client
// missed, so a brief network outage or a slow-consumer disconnect loses no
// data. If the requested ID is no longer available (for example after a
// server restart), the stream simply continues with live events. Use
// [WithReplay] to change the store or to disable replay.
//
// # Slow consumers
//
// Publishing never blocks. Each subscriber has a bounded buffer (see
// [WithBufferSize]); a subscriber that falls too far behind is disconnected and
// will catch up through replay when its browser reconnects.
//
// # Shutdown
//
// Long-lived SSE connections keep [net/http.Server.Shutdown] from completing.
// Call [Broker.Shutdown] first, or register it with
// [net/http.Server.RegisterOnShutdown]:
//
//	srv.RegisterOnShutdown(func() { b.Shutdown(context.Background()) })
package gosse
