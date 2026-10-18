package ssetest

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestReaderSpecExamples(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		want  []Event
		retry int64
	}{
		{
			name: "multi-line data",
			in:   "data: YHOO\ndata: +2\ndata: 10\n\n",
			want: []Event{{Data: "YHOO\n+2\n10"}},
		},
		{
			name: "id persists, comments ignored",
			in:   ": test stream\n\ndata: first event\nid: 1\n\ndata:second event\nid\n\ndata:  third event\n\n",
			want: []Event{
				{LastEventID: "1", Data: "first event"},
				{LastEventID: "", Data: "second event"},
				{LastEventID: "", Data: " third event"},
			},
		},
		{
			name: "empty data fields",
			in:   "data\n\ndata\ndata\n\ndata:",
			want: []Event{{Data: ""}, {Data: "\n"}},
		},
		{
			name: "CR and CRLF line endings, BOM, event type",
			in:   "\uFEFFevent: add\r\ndata: a\rdata: b\r\n\r\n",
			want: []Event{{Type: "add", Data: "a\nb"}},
		},
		{
			name: "no data is not dispatched; type resets",
			in:   "event: x\n\ndata: y\n\n",
			want: []Event{{Data: "y"}},
		},
		{
			name:  "id with NUL ignored, retry",
			in:    "id: 5\nretry: 1500\nretry: 1.5\nid: a\x00b\ndata: z\n\n",
			want:  []Event{{LastEventID: "5", Data: "z"}},
			retry: 1500,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.in))
			var got []Event
			for {
				ev, err := r.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, ev)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v, want %#v", got, tt.want)
			}
			if tt.retry != 0 && r.Retry != tt.retry {
				t.Fatalf("retry = %d, want %d", r.Retry, tt.retry)
			}
		})
	}
}
