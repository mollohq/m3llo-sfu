package logging

import (
	"bytes"
	"encoding/json"
	"testing"
)

// The disk sink must not change the stdout line, and it gets the same
// line without the newline.
func TestSinkGetsTheStdoutLine(t *testing.T) {
	var out bytes.Buffer
	l := NewWithRing("info", 3)
	l.output = &out
	var gotMs int64
	var gotLine []byte
	var gotEntry Entry
	l.SetSink(func(ms int64, e Entry, line []byte) {
		gotMs, gotEntry, gotLine = ms, e, append([]byte(nil), line...)
	})
	l.Info("peer_joined", D{"session": "s1", "user": "u1", "role": "host <b>"})

	var want bytes.Buffer
	_ = json.NewEncoder(&want).Encode(gotEntry)
	if out.String() != want.String() {
		t.Fatalf("stdout line changed:\n got %q\nwant %q", out.String(), want.String())
	}
	if string(gotLine)+"\n" != out.String() {
		t.Fatalf("sink line %q is not the stdout line %q", gotLine, out.String())
	}
	if gotMs <= 0 || gotEntry.SessionID != "s1" || gotEntry.UserID != "u1" {
		t.Fatalf("sink entry = %+v at %d", gotEntry, gotMs)
	}

	l.SetSink(nil)
	gotLine = nil
	l.Info("x", nil)
	if gotLine != nil {
		t.Fatal("a removed sink was called")
	}
}

func TestRingSizeIsConfigurable(t *testing.T) {
	l := NewWithRing("info", 3)
	l.output = &bytes.Buffer{}
	for i := 0; i < 5; i++ {
		l.Info("e", D{"i": i})
	}
	if got := l.Ring().Query(LogFilter{Limit: 10}); got.Total != 3 || len(got.Entries) != 3 {
		t.Fatalf("ring holds %d (total %d), want 3", len(got.Entries), got.Total)
	}
}
