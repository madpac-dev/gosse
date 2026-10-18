package gosse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Event is a single server-sent event.
//
// The broker assigns the event's ID; applications only describe its content.
// All fields are optional. An Event with an empty Data field is still
// delivered to the client (as an event whose data is the empty string).
type Event struct {
	// Type is the event type, sent as the "event:" field. Browsers dispatch
	// events without a type to EventSource.onmessage, and typed events to
	// listeners registered with addEventListener(Type, ...).
	// It must not contain CR or LF characters.
	Type string

	// Data is the event payload. It may contain line breaks (LF, CR or CRLF);
	// each line is sent as its own "data:" field and browsers rejoin them
	// with LF. The stream is UTF-8, so Data should be valid UTF-8.
	Data string

	// Retry, if positive, tells the client how long to wait before
	// reconnecting after the connection is lost. It is sent in
	// milliseconds as the "retry:" field.
	Retry time.Duration

	// Comment is sent as one or more comment lines (starting with ":").
	// Clients ignore comments; they can be useful for debugging.
	Comment string
}

// ErrInvalidEvent is returned (wrapped) by [Broker.Publish] when an [Event]
// cannot be encoded according to the SSE specification.
var ErrInvalidEvent = errors.New("gosse: invalid event")

func (e Event) validate() error {
	if strings.ContainsAny(e.Type, "\r\n") {
		return fmt.Errorf("%w: Type must not contain CR or LF", ErrInvalidEvent)
	}
	if e.Retry < 0 {
		return fmt.Errorf("%w: Retry must not be negative", ErrInvalidEvent)
	}
	return nil
}

// appendEvent appends the wire representation of ev with the given id to buf.
// ev must have been validated.
func appendEvent(buf []byte, id uint64, ev Event) []byte {
	if ev.Comment != "" {
		forEachLine(ev.Comment, func(line string) {
			buf = append(buf, ": "...)
			buf = append(buf, line...)
			buf = append(buf, '\n')
		})
	}
	buf = append(buf, "id: "...)
	buf = strconv.AppendUint(buf, id, 10)
	buf = append(buf, '\n')
	if ev.Type != "" {
		buf = append(buf, "event: "...)
		buf = append(buf, ev.Type...)
		buf = append(buf, '\n')
	}
	if ev.Retry > 0 {
		buf = append(buf, "retry: "...)
		buf = strconv.AppendInt(buf, ev.Retry.Milliseconds(), 10)
		buf = append(buf, '\n')
	}
	// Always emit at least one data line: the spec does not dispatch events
	// whose data buffer is empty.
	forEachLine(ev.Data, func(line string) {
		buf = append(buf, "data: "...)
		buf = append(buf, line...)
		buf = append(buf, '\n')
	})
	return append(buf, '\n')
}

// forEachLine calls fn for every line of s, treating CRLF, LF and CR as line
// terminators. A trailing terminator yields a final empty line, so that the
// client reconstructs s exactly (modulo line-ending normalisation to LF).
func forEachLine(s string, fn func(string)) {
	for {
		i := strings.IndexAny(s, "\r\n")
		if i < 0 {
			fn(s)
			return
		}
		fn(s[:i])
		if s[i] == '\r' && i+1 < len(s) && s[i+1] == '\n' {
			i++
		}
		s = s[i+1:]
	}
}

var heartbeatBytes = []byte(": heartbeat\n\n")
