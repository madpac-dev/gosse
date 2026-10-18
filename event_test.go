package gosse

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/madpac-dev/gosse/internal/ssetest"
)

func TestAppendEvent(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
		want string
	}{
		{"empty", Event{}, "id: 7\ndata: \n\n"},
		{"data", Event{Data: "hello"}, "id: 7\ndata: hello\n\n"},
		{"type", Event{Type: "add", Data: "x"}, "id: 7\nevent: add\ndata: x\n\n"},
		{"LF", Event{Data: "a\nb"}, "id: 7\ndata: a\ndata: b\n\n"},
		{"CR", Event{Data: "a\rb"}, "id: 7\ndata: a\ndata: b\n\n"},
		{"CRLF", Event{Data: "a\r\nb"}, "id: 7\ndata: a\ndata: b\n\n"},
		{"trailing newline", Event{Data: "a\n"}, "id: 7\ndata: a\ndata: \n\n"},
		{"blank lines", Event{Data: "\n\n"}, "id: 7\ndata: \ndata: \ndata: \n\n"},
		{"CR CR LF", Event{Data: "a\r\r\nb"}, "id: 7\ndata: a\ndata: \ndata: b\n\n"},
		{"leading space", Event{Data: " x"}, "id: 7\ndata:  x\n\n"},
		{"retry", Event{Retry: 2500 * time.Millisecond}, "id: 7\nretry: 2500\ndata: \n\n"},
		{"comment", Event{Comment: "hi\nthere", Data: "d"}, ": hi\n: there\nid: 7\ndata: d\n\n"},
		{"all", Event{Comment: "c", Type: "t", Data: "d", Retry: time.Second}, ": c\nid: 7\nevent: t\nretry: 1000\ndata: d\n\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.ev.validate(); err != nil {
				t.Fatal(err)
			}
			if got := string(appendEvent(nil, 7, tt.ev)); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEventValidate(t *testing.T) {
	for _, ev := range []Event{
		{Type: "a\nb"},
		{Type: "a\rb"},
		{Retry: -time.Second},
	} {
		if err := ev.validate(); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("validate(%#v) = %v, want ErrInvalidEvent", ev, err)
		}
	}
}

// normalize converts CRLF and CR to LF, as a client does.
func normalize(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", "\n"), "\r", "\n")
}

func FuzzEventRoundTrip(f *testing.F) {
	f.Add("hello", "add", "note", uint64(1))
	f.Add("a\r\nb\rc\n", "", "", uint64(0))
	f.Add("", " spaced", "\n", uint64(1<<63))
	f.Add("data: nested\n\nid: 3", "x:y", ":", uint64(42))
	f.Fuzz(func(t *testing.T, data, typ, comment string, id uint64) {
		ev := Event{Type: typ, Data: data, Comment: comment}
		if ev.validate() != nil {
			t.Skip()
		}
		wire := appendEvent(nil, id, ev)
		r := ssetest.NewReader(bytes.NewReader(wire))
		got, err := r.Next()
		if err != nil {
			t.Fatalf("parse %q: %v", wire, err)
		}
		if got.Data != normalize(data) {
			t.Fatalf("data = %q, want %q (wire %q)", got.Data, normalize(data), wire)
		}
		if got.Type != typ {
			t.Fatalf("type = %q, want %q", got.Type, typ)
		}
		if want := strconv.FormatUint(id, 10); got.LastEventID != want {
			t.Fatalf("id = %q, want %q", got.LastEventID, want)
		}
		if _, err := r.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected exactly one event, wire %q", wire)
		}
	})
}
