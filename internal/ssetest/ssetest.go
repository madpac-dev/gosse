// Package ssetest implements an event-stream parser that follows the
// WHATWG "Server-sent events" interpretation rules
// (https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation).
// It is used to verify gosse's output the way a browser would see it.
package ssetest

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
)

// Event is a dispatched event as an EventSource would expose it.
type Event struct {
	LastEventID string // the last event ID string at dispatch time
	Type        string // event type; "" means the default ("message")
	Data        string
}

// Reader parses an event stream.
type Reader struct {
	r           *bufio.Reader
	started     bool
	lastEventID string
	data        strings.Builder
	hasData     bool
	typ         string

	// Retry is the reconnection time in milliseconds last set by the
	// stream, or -1 if never set.
	Retry int64
	// Comments receives every comment line, without the leading colon.
	Comments []string
}

// NewReader returns a Reader that parses r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r), Retry: -1}
}

// Next returns the next dispatched event. At the end of the stream it
// returns io.EOF; an incomplete trailing event is discarded, as per the spec.
func (p *Reader) Next() (Event, error) {
	for {
		line, err := p.readLine()
		if err != nil {
			return Event{}, err
		}
		if line == "" {
			if ev, ok := p.dispatch(); ok {
				return ev, nil
			}
			continue
		}
		if line[0] == ':' {
			p.Comments = append(p.Comments, line[1:])
			continue
		}
		field, value := line, ""
		if i := strings.IndexByte(line, ':'); i >= 0 {
			field, value = line[:i], strings.TrimPrefix(line[i+1:], " ")
		}
		p.process(field, value)
	}
}

func (p *Reader) process(field, value string) {
	switch field {
	case "event":
		p.typ = value
	case "data":
		p.data.WriteString(value)
		p.data.WriteByte('\n')
		p.hasData = true
	case "id":
		if !strings.ContainsRune(value, 0) {
			p.lastEventID = value
		}
	case "retry":
		if value != "" && strings.Trim(value, "0123456789") == "" {
			if n, err := strconv.ParseInt(value, 10, 64); err == nil {
				p.Retry = n
			}
		}
	}
}

func (p *Reader) dispatch() (Event, bool) {
	defer func() {
		p.data.Reset()
		p.hasData = false
		p.typ = ""
	}()
	if !p.hasData {
		return Event{}, false
	}
	data := strings.TrimSuffix(p.data.String(), "\n")
	return Event{LastEventID: p.lastEventID, Type: p.typ, Data: data}, true
}

// readLine reads a line terminated by CRLF, LF or CR, stripping a leading
// UTF-8 BOM from the stream.
func (p *Reader) readLine() (string, error) {
	var buf bytes.Buffer
	for {
		c, err := p.r.ReadByte()
		if err != nil {
			return "", err
		}
		switch c {
		case '\n':
			return p.finish(buf.String()), nil
		case '\r':
			if next, err := p.r.Peek(1); err == nil && next[0] == '\n' {
				_, _ = p.r.ReadByte()
			}
			return p.finish(buf.String()), nil
		default:
			buf.WriteByte(c)
		}
	}
}

func (p *Reader) finish(line string) string {
	if !p.started {
		p.started = true
		line = strings.TrimPrefix(line, "\uFEFF")
	}
	return line
}
