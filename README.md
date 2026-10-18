# gosse

[![Go Reference](https://pkg.go.dev/badge/github.com/madpac-dev/gosse.svg)](https://pkg.go.dev/github.com/madpac-dev/gosse)
[![CI](https://github.com/madpac-dev/gosse/actions/workflows/ci.yml/badge.svg)](https://github.com/madpac-dev/gosse/actions/workflows/ci.yml)

Server-Sent Events for any Go `net/http` server, with zero configuration.

- **Follows the [WHATWG SSE spec](https://html.spec.whatwg.org/multipage/server-sent-events.html)**: works with a plain browser `EventSource`.
- **Reconnection works out of the box:** after a reconnect, missed events are resent from the `Last-Event-ID` the browser sends back.
- **Topics:** a client can subscribe to several topics and still receives each event once.
- **Publishing never blocks:** clients that fall too far behind are disconnected and catch up on reconnect.
- **Heartbeats** keep proxies from closing idle connections.
- **Graceful shutdown.**
- **No dependencies** beyond the standard library. Requires Go 1.23+.

## Install

```sh
go get github.com/madpac-dev/gosse
```

## Usage

```go
b := gosse.NewBroker()

mux.HandleFunc("GET /lists/{id}/events", func(w http.ResponseWriter, r *http.Request) {
    id := r.PathValue("id")
    if !canView(r, id) { // your own auth
        http.Error(w, "forbidden", http.StatusForbidden)
        return
    }
    b.Serve(w, r, "list:"+id)
})

// After saving an item:
b.PublishJSON("list:"+id, "item-added", item)
```

```js
const es = new EventSource("/lists/42/events");
es.addEventListener("item-added", (e) => console.log(JSON.parse(e.data)));
```

That's it. For full control over an event, use `Publish`:

```go
b.Publish(ctx, gosse.Event{Type: "notice", Data: "line one\nline two"}, "list:1", "list:2")
```

See [`examples/shoppinglist`](examples/shoppinglist) for a complete app:

```sh
go run ./examples/shoppinglist   # open http://localhost:8080/lists/groceries in two windows
```

## How it works

| Concern | Behaviour |
|---|---|
| Routing & auth | Your handler decides the topics and calls `Serve`. gosse never reads topics from the request. |
| Event IDs | Assigned by the broker, increasing, and greater than any ID issued before a restart. |
| Replay | The last 1000 events are kept in memory. On reconnect, the events the client missed (on its topics) are sent before live events, with no gaps or duplicates. If the ID is unknown or too old, the stream continues live. |
| Slow clients | Each client has a 64-event buffer. On overflow the client is disconnected and recovers via replay. |
| Heartbeat | A `: heartbeat` comment every 15s. |
| Server timeouts | `Serve` clears the connection's read/write deadlines, so `http.Server` `ReadTimeout`/`WriteTimeout` don't cut streams. Each write still has its own timeout, so vanished clients are cleaned up. |
| Middleware | Works through wrapping `ResponseWriter`s that implement `Unwrap()` (via `http.ResponseController`). |

## Options

All optional:

```go
gosse.NewBroker(
    gosse.WithReplay(gosse.NewMemoryReplayStore(10_000)), // or nil to disable; or your own ReplayStore
    gosse.WithHeartbeat(30*time.Second),                  // 0 disables
    gosse.WithBufferSize(256),
    gosse.WithLogger(slog.Default()),
)
```

## Shutdown

Open SSE connections block `http.Server.Shutdown`. Close them first:

```go
srv.RegisterOnShutdown(func() {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    b.Shutdown(ctx)
})
```

## Scope

gosse is server-side and single-process. To fan out across several instances, call `Publish` on each instance (for example from a Redis or NATS subscription).

## License

[MIT](LICENSE)
