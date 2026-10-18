package gosse

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/madpac-dev/gosse/internal/ssetest"
)

const testTimeout = 5 * time.Second

// newServer serves b on an httptest server; the subscriber's topics are
// taken from repeated ?topic= parameters (test convenience only).
func newServer(t *testing.T, b *Broker) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.Serve(w, r, r.URL.Query()["topic"]...)
	}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		_ = b.Shutdown(ctx)
		srv.Close()
	})
	return srv
}

type client struct {
	resp   *http.Response
	events chan ssetest.Event
	done   chan struct{} // closed when the stream ends
	cancel context.CancelFunc
}

func connect(t *testing.T, srv *httptest.Server, lastID string, topics ...string) *client {
	t.Helper()
	q := url.Values{"topic": topics}
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"?"+q.Encode(), nil)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	c := &client{resp: resp, events: make(chan ssetest.Event, 100), done: make(chan struct{}), cancel: cancel}
	t.Cleanup(func() { cancel(); resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		close(c.done)
		return c
	}
	go func() {
		defer close(c.done)
		r := ssetest.NewReader(resp.Body)
		for {
			ev, err := r.Next()
			if err != nil {
				return
			}
			c.events <- ev
		}
	}()
	return c
}

func (c *client) next(t *testing.T) ssetest.Event {
	t.Helper()
	select {
	case ev := <-c.events:
		return ev
	case <-c.done:
		select {
		case ev := <-c.events:
			return ev
		default:
		}
		t.Fatal("stream ended while waiting for event")
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for event")
	}
	return ssetest.Event{}
}

func (c *client) expectNone(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case ev := <-c.events:
		t.Fatalf("unexpected event %#v", ev)
	case <-time.After(d):
	}
}

func (c *client) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(testTimeout):
		t.Fatal("stream did not close")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func publish(t *testing.T, b *Broker, ev Event, topics ...string) {
	t.Helper()
	if err := b.Publish(context.Background(), ev, topics...); err != nil {
		t.Fatal(err)
	}
}

func TestServeHeaders(t *testing.T) {
	b := NewBroker()
	srv := newServer(t, b)
	c := connect(t, srv, "", "a")
	h := c.resp.Header
	for k, want := range map[string]string{
		"Content-Type":      "text/event-stream",
		"Cache-Control":     "no-cache",
		"X-Accel-Buffering": "no",
		"Connection":        "keep-alive",
	} {
		if got := h.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if b.subscriberCount() != 1 {
		t.Errorf("subscriberCount = %d, want 1 once headers are received", b.subscriberCount())
	}
}

func TestPublishDelivers(t *testing.T) {
	b := NewBroker()
	srv := newServer(t, b)
	c := connect(t, srv, "", "list:1")

	publish(t, b, Event{Type: "item-added", Data: "milk\neggs"}, "list:1")
	if err := b.PublishJSON("list:1", "item-removed", map[string]string{"name": "milk"}); err != nil {
		t.Fatal(err)
	}

	e1, e2 := c.next(t), c.next(t)
	if e1.Type != "item-added" || e1.Data != "milk\neggs" {
		t.Errorf("first event = %#v", e1)
	}
	if e2.Type != "item-removed" || e2.Data != `{"name":"milk"}` {
		t.Errorf("second event = %#v", e2)
	}
	id1, err1 := strconv.ParseUint(e1.LastEventID, 10, 64)
	id2, err2 := strconv.ParseUint(e2.LastEventID, 10, 64)
	if err1 != nil || err2 != nil || id2 != id1+1 {
		t.Errorf("ids %q, %q should be consecutive integers", e1.LastEventID, e2.LastEventID)
	}
}

func TestIDsGreaterThanPreviousBroker(t *testing.T) {
	b1 := NewBroker()
	_ = b1.Publish(context.Background(), Event{}, "a")
	last := b1.seq
	time.Sleep(time.Millisecond)
	if b2 := NewBroker(); b2.seq <= last {
		t.Fatalf("new broker seq %d not greater than previous %d", b2.seq, last)
	}
}

func TestTopicFilteringAndDedupe(t *testing.T) {
	b := NewBroker()
	srv := newServer(t, b)
	ab := connect(t, srv, "", "a", "b", "a")
	onlyB := connect(t, srv, "", "b")

	publish(t, b, Event{Data: "1"}, "a")
	publish(t, b, Event{Data: "2"}, "a", "b", "b")
	publish(t, b, Event{Data: "3"}, "c")
	publish(t, b, Event{Data: "4"}, "b")

	for _, want := range []string{"1", "2", "4"} {
		if got := ab.next(t).Data; got != want {
			t.Fatalf("ab got %q, want %q", got, want)
		}
	}
	for _, want := range []string{"2", "4"} {
		if got := onlyB.next(t).Data; got != want {
			t.Fatalf("onlyB got %q, want %q", got, want)
		}
	}
	ab.expectNone(t, 50*time.Millisecond)
	onlyB.expectNone(t, 0)
}

func TestReplay(t *testing.T) {
	b := NewBroker()
	srv := newServer(t, b)
	first := connect(t, srv, "", "a")
	publish(t, b, Event{Data: "1"}, "a")
	publish(t, b, Event{Data: "2"}, "b")
	publish(t, b, Event{Data: "3"}, "a", "b")
	publish(t, b, Event{Data: "4"}, "a")
	lastID := first.next(t).LastEventID // event "1"

	c := connect(t, srv, lastID, "a")
	publish(t, b, Event{Data: "5"}, "a")
	for _, want := range []string{"3", "4", "5"} {
		if got := c.next(t).Data; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	c.expectNone(t, 50*time.Millisecond)
}

// TestReplayNoDuplicates publishes continuously while clients reconnect and
// checks that every client sees a gap-free, duplicate-free ID sequence.
func TestReplayNoDuplicates(t *testing.T) {
	b := NewBroker(WithBufferSize(10000))
	srv := newServer(t, b)
	first := connect(t, srv, "", "a")
	publish(t, b, Event{}, "a")
	start := first.next(t).LastEventID

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			_ = b.Publish(context.Background(), Event{}, "a")
		}
	}()
	for i := 0; i < 5; i++ {
		c := connect(t, srv, start, "a")
		prev, _ := strconv.ParseUint(start, 10, 64)
		for j := 0; j < 50; j++ {
			id, _ := strconv.ParseUint(c.next(t).LastEventID, 10, 64)
			if id != prev+1 {
				t.Fatalf("client %d: got id %d after %d", i, id, prev)
			}
			prev = id
		}
		c.cancel()
	}
	wg.Wait()
}

func TestReplayGap(t *testing.T) {
	b := NewBroker(WithReplay(NewMemoryReplayStore(2)))
	srv := newServer(t, b)
	first := connect(t, srv, "", "a")
	for i := 1; i <= 5; i++ {
		publish(t, b, Event{Data: strconv.Itoa(i)}, "a")
	}
	lastID := first.next(t).LastEventID // "1"; 2 and 3 are evicted

	c := connect(t, srv, lastID, "a")
	publish(t, b, Event{Data: "live"}, "a")
	if got := c.next(t).Data; got != "live" {
		t.Fatalf("got %q, want only live events after a gap", got)
	}
}

func TestReplayDisabled(t *testing.T) {
	b := NewBroker(WithReplay(nil))
	srv := newServer(t, b)
	first := connect(t, srv, "", "a")
	publish(t, b, Event{Data: "1"}, "a")
	publish(t, b, Event{Data: "2"}, "a")
	e := first.next(t)
	if e.LastEventID == "" {
		t.Fatal("IDs must be sent even when replay is disabled")
	}
	c := connect(t, srv, e.LastEventID, "a")
	publish(t, b, Event{Data: "3"}, "a")
	if got := c.next(t).Data; got != "3" {
		t.Fatalf("got %q, want 3", got)
	}
}

func TestReplayInvalidLastEventID(t *testing.T) {
	b := NewBroker()
	srv := newServer(t, b)
	publish(t, b, Event{Data: "old"}, "a")
	c := connect(t, srv, "not-a-number", "a")
	publish(t, b, Event{Data: "new"}, "a")
	if got := c.next(t).Data; got != "new" {
		t.Fatalf("got %q, want new", got)
	}
}

type failingStore struct{}

func (failingStore) Append(uint64, Event, []string) error { return errors.New("append boom") }
func (failingStore) Since(uint64, []string) ([]StoredEvent, bool, error) {
	return nil, false, errors.New("since boom")
}

func TestReplayStoreErrors(t *testing.T) {
	b := NewBroker(WithReplay(failingStore{}))
	srv := newServer(t, b)
	c := connect(t, srv, "123", "a")
	err := b.Publish(context.Background(), Event{Data: "x"}, "a")
	if err == nil || !strings.Contains(err.Error(), "append boom") {
		t.Fatalf("Publish error = %v, want store error", err)
	}
	if got := c.next(t).Data; got != "x" {
		t.Fatalf("event must be delivered despite store error, got %q", got)
	}
}

func TestHeartbeat(t *testing.T) {
	b := NewBroker(WithHeartbeat(10 * time.Millisecond))
	srv := newServer(t, b)

	resp, err := srv.Client().Get(srv.URL + "?topic=a")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	for i := 0; i < 2; i++ {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line != ": heartbeat\n" {
			t.Fatalf("line %q, want heartbeat comment", line)
		}
		if blank, _ := br.ReadString('\n'); blank != "\n" {
			t.Fatalf("expected blank line after heartbeat, got %q", blank)
		}
	}
}

func TestServerTimeoutsDoNotCutStream(t *testing.T) {
	b := NewBroker(WithHeartbeat(0))
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.Serve(w, r, "a")
	}))
	srv.Config.ReadTimeout = 50 * time.Millisecond
	srv.Config.WriteTimeout = 50 * time.Millisecond
	srv.Start()
	t.Cleanup(func() { _ = b.Shutdown(context.Background()); srv.Close() })

	c := connect(t, srv, "", "a")
	time.Sleep(150 * time.Millisecond)
	publish(t, b, Event{Data: "late"}, "a")
	if got := c.next(t).Data; got != "late" {
		t.Fatalf("got %q", got)
	}
}

func TestServeNoTopics(t *testing.T) {
	b := NewBroker()
	srv := newServer(t, b)
	c := connect(t, srv, "")
	if c.resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", c.resp.StatusCode)
	}
}

type plainWriter struct {
	h    http.Header
	code int
	body bytes.Buffer
}

func (w *plainWriter) Header() http.Header         { return w.h }
func (w *plainWriter) WriteHeader(code int)        { w.code = code }
func (w *plainWriter) Write(p []byte) (int, error) { return w.body.Write(p) }

func TestServeNotFlushable(t *testing.T) {
	b := NewBroker()
	w := &plainWriter{h: http.Header{}}
	b.Serve(w, httptest.NewRequest(http.MethodGet, "/", nil), "a")
	if w.code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.code)
	}
}

// unwrapper hides the Flusher of the wrapped writer behind Unwrap, like many
// middleware response writers do.
type unwrapper struct{ http.ResponseWriter }

func (u unwrapper) Unwrap() http.ResponseWriter { return u.ResponseWriter }

func TestServeThroughMiddlewareWrapper(t *testing.T) {
	b := NewBroker()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.Serve(unwrapper{w}, r, "a")
	}))
	t.Cleanup(func() { _ = b.Shutdown(context.Background()); srv.Close() })
	c := connect(t, srv, "", "a")
	publish(t, b, Event{Data: "ok"}, "a")
	if got := c.next(t).Data; got != "ok" {
		t.Fatalf("got %q", got)
	}
}

// blockingWriter is a flushable ResponseWriter whose Write blocks until gate
// is closed.
type blockingWriter struct {
	h    http.Header
	gate chan struct{}
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{h: http.Header{}, gate: make(chan struct{})}
}
func (w *blockingWriter) Header() http.Header         { return w.h }
func (w *blockingWriter) WriteHeader(int)             {}
func (w *blockingWriter) Write(p []byte) (int, error) { <-w.gate; return len(p), nil }
func (w *blockingWriter) Flush()                      {}

func serveAsync(b *Broker, w http.ResponseWriter, ctx context.Context, topics ...string) chan struct{} {
	done := make(chan struct{})
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	go func() {
		defer close(done)
		b.Serve(w, r, topics...)
	}()
	return done
}

func TestSlowConsumerDisconnected(t *testing.T) {
	var logs bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(lockedWriter{&mu, &logs}, nil))
	b := NewBroker(WithBufferSize(1), WithHeartbeat(0), WithLogger(logger))
	w := newBlockingWriter()
	done := serveAsync(b, w, context.Background(), "a")
	waitFor(t, func() bool { return b.subscriberCount() == 1 })

	// One event may be in flight (blocked in Write) and one buffered; the
	// third overflows.
	for i := 0; i < 3; i++ {
		publish(t, b, Event{}, "a")
	}
	if n := b.subscriberCount(); n != 0 {
		t.Fatalf("subscriberCount = %d, want 0 after overflow", n)
	}
	publish(t, b, Event{}, "a") // must not panic or block
	close(w.gate)
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Serve did not return")
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(logs.String(), "slow subscriber") {
		t.Errorf("expected slow subscriber log, got %q", logs.String())
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func TestClientDisconnectCleansUp(t *testing.T) {
	b := NewBroker()
	srv := newServer(t, b)
	c := connect(t, srv, "", "a", "b")
	if b.subscriberCount() != 1 {
		t.Fatal("expected one subscriber")
	}
	c.cancel()
	waitFor(t, func() bool { return b.subscriberCount() == 0 })
	b.mu.Lock()
	n := len(b.topics)
	b.mu.Unlock()
	if n != 0 {
		t.Fatalf("topics map not cleaned up: %d entries", n)
	}
}

func TestShutdown(t *testing.T) {
	b := NewBroker()
	srv := newServer(t, b)
	c1 := connect(t, srv, "", "a")
	c2 := connect(t, srv, "", "b")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	c1.waitClosed(t)
	c2.waitClosed(t)

	if err := b.Publish(context.Background(), Event{}, "a"); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("Publish after Shutdown = %v, want ErrBrokerClosed", err)
	}
	if c := connect(t, srv, "", "a"); c.resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", c.resp.StatusCode)
	}
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown = %v", err)
	}
}

func TestShutdownTimeout(t *testing.T) {
	b := NewBroker(WithHeartbeat(0))
	w := newBlockingWriter()
	done := serveAsync(b, w, context.Background(), "a")
	waitFor(t, func() bool { return b.subscriberCount() == 1 })
	publish(t, b, Event{}, "a")
	time.Sleep(10 * time.Millisecond) // let Serve block in Write

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
	}
	close(w.gate)
	<-done
	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown after drain = %v", err)
	}
}

func TestPublishErrors(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()
	if err := b.Publish(ctx, Event{}); !errors.Is(err, ErrNoTopics) {
		t.Errorf("no topics: %v", err)
	}
	if err := b.Publish(ctx, Event{Type: "a\nb"}, "t"); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("invalid event: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := b.Publish(canceled, Event{}, "t"); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled ctx: %v", err)
	}
	if err := b.PublishJSON("t", "x", make(chan int)); err == nil {
		t.Error("PublishJSON with unencodable value must fail")
	}
}

func TestOptionPanics(t *testing.T) {
	for name, f := range map[string]func(){
		"heartbeat": func() { WithHeartbeat(-1) },
		"buffer":    func() { WithBufferSize(0) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			f()
		})
	}
}

func TestConcurrentPublishAndSubscribe(t *testing.T) {
	b := NewBroker(WithBufferSize(1000))
	srv := newServer(t, b)
	const publishers, perPublisher = 4, 50

	var clients []*client
	for i := 0; i < 5; i++ {
		clients = append(clients, connect(t, srv, "", "x", "y"))
	}
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				_ = b.PublishJSON([]string{"x", "y"}[i%2], "n", i)
			}
		}()
	}
	// Churn: subscribers coming and going while publishing.
	for i := 0; i < 10; i++ {
		connect(t, srv, "", "x").cancel()
	}
	wg.Wait()
	for _, c := range clients {
		var prev uint64
		for i := 0; i < publishers*perPublisher; i++ {
			id, _ := strconv.ParseUint(c.next(t).LastEventID, 10, 64)
			if id <= prev {
				t.Fatalf("ids not increasing: %d after %d", id, prev)
			}
			prev = id
		}
	}
}
