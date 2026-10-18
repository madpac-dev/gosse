package gosse

import (
	"reflect"
	"testing"
)

func ids(evs []StoredEvent) []uint64 {
	out := []uint64{}
	for _, e := range evs {
		out = append(out, e.ID)
	}
	return out
}

func TestMemoryReplayStore(t *testing.T) {
	s := NewMemoryReplayStore(3)

	if _, found, _ := s.Since(1, []string{"a"}); found {
		t.Fatal("empty store must not report found")
	}

	mustAppend := func(id uint64, topics ...string) {
		t.Helper()
		if err := s.Append(id, Event{Data: "x"}, topics); err != nil {
			t.Fatal(err)
		}
	}
	mustAppend(10, "a")
	mustAppend(11, "b")
	mustAppend(12, "a", "b")

	tests := []struct {
		last   uint64
		topics []string
		want   []uint64
		found  bool
	}{
		{9, []string{"a"}, []uint64{10, 12}, true},
		{9, []string{"b"}, []uint64{11, 12}, true},
		{10, []string{"a", "b"}, []uint64{11, 12}, true},
		{12, []string{"a"}, []uint64{}, true},
		{13, []string{"a"}, []uint64{}, false},
		{8, []string{"a"}, []uint64{}, false},
		{9, []string{"c"}, []uint64{}, true},
	}
	for _, tt := range tests {
		got, found, err := s.Since(tt.last, tt.topics)
		if err != nil {
			t.Fatal(err)
		}
		if found != tt.found || !reflect.DeepEqual(ids(got), tt.want) {
			t.Errorf("Since(%d, %v) = %v, %v; want %v, %v", tt.last, tt.topics, ids(got), found, tt.want, tt.found)
		}
	}

	// Wrap around: 10 is evicted.
	mustAppend(13, "a")
	if s.Len() != 3 {
		t.Fatalf("Len = %d, want 3", s.Len())
	}
	if _, found, _ := s.Since(9, []string{"a"}); found {
		t.Error("evicted ID must not be found")
	}
	got, found, _ := s.Since(10, []string{"a"})
	if !found || !reflect.DeepEqual(ids(got), []uint64{12, 13}) {
		t.Errorf("after wrap Since(10) = %v, %v", ids(got), found)
	}
	if got[0].Data != "x" {
		t.Errorf("event content lost: %#v", got[0])
	}

	if err := s.Append(13, Event{}, []string{"a"}); err == nil {
		t.Error("non-increasing ID must be rejected")
	}
}

func TestNewMemoryReplayStorePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	NewMemoryReplayStore(0)
}
