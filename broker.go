package gosse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Errors returned by [Broker] methods.
var (
	// ErrBrokerClosed is returned by Publish after Shutdown has been called.
	ErrBrokerClosed = errors.New("gosse: broker closed")
	// ErrNoTopics is returned by Publish when called without any topic.
	ErrNoTopics = errors.New("gosse: no topics")
)

// defaultWriteTimeout bounds how long a single write to a client may block,
// so that a vanished client cannot pin a goroutine indefinitely.
const defaultWriteTimeout = 30 * time.Second

// Broker fans out published events to subscribed SSE clients.
//
// A Broker is safe for concurrent use. Create one with [NewBroker]; the zero
// value is not usable.
type Broker struct {
	replay       ReplayStore
	heartbeat    time.Duration
	bufferSize   int
	log          *slog.Logger
	writeTimeout time.Duration

	mu     sync.Mutex
	seq    uint64                              // last assigned event ID
	topics map[string]map[*subscriber]struct{} // topic -> subscribers
	closed bool
	done   chan struct{} // closed by Shutdown
	wg     sync.WaitGroup
}

type message struct {
	id   uint64
	data []byte // encoded event, shared by all subscribers; read-only
}

type subscriber struct {
	topics []string
	ch     chan message
	kick   chan struct{} // closed when disconnected for being too slow
	kicked bool          // guarded by Broker.mu
}

// NewBroker returns a ready-to-use Broker. Without options it keeps the last
// [DefaultReplayCapacity] events for replay, sends heartbeats every
// [DefaultHeartbeat] and buffers up to [DefaultBufferSize] events per
// subscriber.
func NewBroker(opts ...Option) *Broker {
	cfg := config{
		heartbeat:    DefaultHeartbeat,
		bufferSize:   DefaultBufferSize,
		writeTimeout: defaultWriteTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if !cfg.replaySet {
		cfg.replay = NewMemoryReplayStore(DefaultReplayCapacity)
	}
	if cfg.logger == nil {
		cfg.logger = slog.New(discardHandler{})
	}
	return &Broker{
		replay:       cfg.replay,
		heartbeat:    cfg.heartbeat,
		bufferSize:   cfg.bufferSize,
		log:          cfg.logger,
		writeTimeout: cfg.writeTimeout,
		// IDs start from the current time in microseconds so that IDs
		// issued after a restart are greater than any issued before it.
		// A client reconnecting with an ID from a previous process thus
		// never has newer events wrongly skipped or replayed.
		seq:    uint64(time.Now().UnixMicro()),
		topics: make(map[string]map[*subscriber]struct{}),
		done:   make(chan struct{}),
	}
}

// Publish sends ev to every client subscribed to at least one of topics.
// Each client receives the event at most once, even if subscribed to several
// of the topics.
//
// Publish never blocks on slow clients. It returns [ErrNoTopics] if no topic
// is given, [ErrBrokerClosed] after [Broker.Shutdown], an error wrapping
// [ErrInvalidEvent] if ev cannot be encoded, or ctx.Err() if ctx is already
// done. If the replay store fails, the event is still delivered to connected
// clients and the store's error is returned.
func (b *Broker) Publish(ctx context.Context, ev Event, topics ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ev.validate(); err != nil {
		return err
	}
	topics = uniqueTopics(topics)
	if len(topics) == 0 {
		return ErrNoTopics
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBrokerClosed
	}
	b.seq++
	id := b.seq

	var storeErr error
	if b.replay != nil {
		if err := b.replay.Append(id, ev, topics); err != nil {
			storeErr = fmt.Errorf("gosse: replay store: %w", err)
			b.log.Error("gosse: replay store append failed", "id", id, "error", err)
		}
	}

	msg := message{id: id, data: appendEvent(nil, id, ev)}
	var seen map[*subscriber]struct{}
	if len(topics) > 1 {
		seen = make(map[*subscriber]struct{})
	}
	for _, t := range topics {
		for s := range b.topics[t] {
			if seen != nil {
				if _, ok := seen[s]; ok {
					continue
				}
				seen[s] = struct{}{}
			}
			select {
			case s.ch <- msg:
			default:
				b.kickLocked(s)
			}
		}
	}
	return storeErr
}

// PublishJSON publishes v, encoded as JSON, with the given event type to a
// single topic. It is shorthand for calling [Broker.Publish] with
// context.Background() and an [Event] whose Data is the JSON encoding of v.
func (b *Broker) PublishJSON(topic, eventType string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("gosse: encoding JSON: %w", err)
	}
	return b.Publish(context.Background(), Event{Type: eventType, Data: string(data)}, topic)
}

// Serve streams events published to any of topics to the client making
// request r. Call it from your own handler, after routing and authorization.
// Serve blocks until the client disconnects, the client is disconnected for
// being too slow, or [Broker.Shutdown] is called.
//
// If r carries a Last-Event-ID header and replay is enabled, the events the
// client missed are sent first.
//
// Serve responds with 400 Bad Request if no topic is given, 500 Internal
// Server Error if w does not support flushing, and 503 Service Unavailable
// after Shutdown. Serve clears any read and write deadlines set by the
// server (for example http.Server.WriteTimeout) for the connection, since
// those would otherwise cut the stream short.
func (b *Broker) Serve(w http.ResponseWriter, r *http.Request, topics ...string) {
	topics = uniqueTopics(topics)
	if len(topics) == 0 {
		b.log.Error("gosse: Serve called without topics", "path", r.URL.Path)
		http.Error(w, "no topics", http.StatusBadRequest)
		return
	}
	if !canFlush(w) {
		b.log.Error("gosse: ResponseWriter does not support flushing", "path", r.URL.Path)
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	sub := &subscriber{
		topics: topics,
		ch:     make(chan message, b.bufferSize),
		kick:   make(chan struct{}),
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	for _, t := range topics {
		m := b.topics[t]
		if m == nil {
			m = make(map[*subscriber]struct{})
			b.topics[t] = m
		}
		m[sub] = struct{}{}
	}
	b.wg.Add(1)
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.removeLocked(sub)
		b.mu.Unlock()
		b.wg.Done()
	}()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // disable buffering in nginx
	if r.ProtoMajor == 1 {
		h.Set("Connection", "keep-alive") // forbidden in HTTP/2 and later
	}
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})
	if err := rc.Flush(); err != nil {
		return
	}

	// The subscription is registered before replaying, so no event can fall
	// between the two; events that appear in both are skipped using lastSent.
	var lastSent uint64
	if b.replay != nil {
		if lastID, ok := parseLastEventID(r); ok {
			events, found, err := b.replay.Since(lastID, topics)
			if err != nil {
				b.log.Error("gosse: replay store lookup failed", "last_event_id", lastID, "error", err)
			}
			if !found {
				b.log.Info("gosse: cannot replay, Last-Event-ID unknown or evicted", "last_event_id", lastID, "topics", topics)
				events = nil
			}
			var buf []byte
			for _, e := range events {
				buf = appendEvent(buf, e.ID, e.Event)
				lastSent = e.ID
			}
			if len(buf) > 0 && !b.write(w, rc, buf) {
				return
			}
		}
	}

	var tick <-chan time.Time
	if b.heartbeat > 0 {
		t := time.NewTicker(b.heartbeat)
		defer t.Stop()
		tick = t.C
	}
	var buf []byte
	for {
		select {
		case <-r.Context().Done():
			return
		case <-b.done:
			return
		case <-sub.kick:
			return
		case <-tick:
			if !b.write(w, rc, heartbeatBytes) {
				return
			}
		case m := <-sub.ch:
			buf = buf[:0]
			if m.id > lastSent {
				buf = append(buf, m.data...)
				lastSent = m.id
			}
			// Coalesce whatever else is already queued into one write.
		drain:
			for {
				select {
				case m := <-sub.ch:
					if m.id > lastSent {
						buf = append(buf, m.data...)
						lastSent = m.id
					}
				default:
					break drain
				}
			}
			if len(buf) > 0 && !b.write(w, rc, buf) {
				return
			}
		}
	}
}

// Shutdown stops the broker: new subscribers are rejected with 503 Service
// Unavailable, Publish returns [ErrBrokerClosed], and all active streams are
// closed. Shutdown waits until every [Broker.Serve] call has returned or ctx is
// done, in which case it returns ctx.Err(). It is safe to call more than once.
//
// Browsers will try to reconnect after their stream closes; behind a load
// balancer they reach another instance, otherwise they receive the 503 and
// stop.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		close(b.done)
	}
	b.mu.Unlock()

	finished := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// write sends p to the client and flushes it. It reports whether the write
// succeeded.
func (b *Broker) write(w http.ResponseWriter, rc *http.ResponseController, p []byte) bool {
	_ = rc.SetWriteDeadline(time.Now().Add(b.writeTimeout))
	_, err := w.Write(p)
	if err == nil {
		err = rc.Flush()
	}
	_ = rc.SetWriteDeadline(time.Time{})
	if err != nil {
		b.log.Debug("gosse: write failed", "error", err)
		return false
	}
	return true
}

// kickLocked disconnects a subscriber that cannot keep up. b.mu must be held.
func (b *Broker) kickLocked(s *subscriber) {
	if s.kicked {
		return
	}
	s.kicked = true
	close(s.kick)
	b.removeLocked(s)
	b.log.Warn("gosse: disconnecting slow subscriber", "topics", s.topics, "buffer_size", b.bufferSize)
}

// removeLocked unregisters s from all its topics. b.mu must be held.
func (b *Broker) removeLocked(s *subscriber) {
	for _, t := range s.topics {
		m := b.topics[t]
		delete(m, s)
		if len(m) == 0 {
			delete(b.topics, t)
		}
	}
}

// subscriberCount returns the number of distinct connected subscribers.
func (b *Broker) subscriberCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	seen := make(map[*subscriber]struct{})
	for _, m := range b.topics {
		for s := range m {
			seen[s] = struct{}{}
		}
	}
	return len(seen)
}

// uniqueTopics returns a new slice with the distinct topics in order of first
// appearance.
func uniqueTopics(topics []string) []string {
	out := make([]string, 0, len(topics))
outer:
	for _, t := range topics {
		for _, o := range out {
			if o == t {
				continue outer
			}
		}
		out = append(out, t)
	}
	return out
}

func parseLastEventID(r *http.Request) (uint64, bool) {
	v := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if v == "" {
		return 0, false
	}
	id, err := strconv.ParseUint(v, 10, 64)
	return id, err == nil
}

// canFlush reports whether w, or a ResponseWriter it wraps, supports
// flushing. It mirrors the unwrapping done by http.ResponseController.
func canFlush(w http.ResponseWriter) bool {
	for {
		switch t := w.(type) {
		case interface{ FlushError() error }:
			return true
		case http.Flusher:
			return true
		case interface{ Unwrap() http.ResponseWriter }:
			w = t.Unwrap()
		default:
			return false
		}
	}
}
