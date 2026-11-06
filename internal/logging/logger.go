package logging

import (
	"encoding/json"
	"io"
	"os"
	"sync/atomic"
	"time"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "unknown"
	}
}

func ParseLevel(s string) Level {
	switch s {
	case "debug":
		return LevelDebug
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

type Entry struct {
	Timestamp string                 `json:"ts"`
	Level     string                 `json:"level"`
	Event     string                 `json:"event"`
	SessionID string                 `json:"session,omitempty"`
	UserID    string                 `json:"user,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

type Logger struct {
	ring     *Ring
	output   io.Writer
	minLevel Level
	sink     atomic.Pointer[Sink]
}

// Sink receives every entry after the ring: the time in unix milliseconds,
// the entry and its JSON line (the stdout line without the newline). It runs
// on the caller's goroutine and must not block; entry.Data is valid only
// during the call.
type Sink func(ms int64, entry Entry, line []byte)

func New(level string) *Logger {
	return NewWithRing(level, DefaultRingSize)
}

// NewWithRing is New with a ring of size entries (SFU_LOG_RING_SIZE).
func NewWithRing(level string, size int) *Logger {
	if size <= 0 {
		size = DefaultRingSize
	}
	return &Logger{
		ring:     NewRing(size),
		output:   os.Stdout,
		minLevel: ParseLevel(level),
	}
}

// SetSink installs (or, with nil, removes) the sink of the disk store.
func (l *Logger) SetSink(sink Sink) {
	if sink == nil {
		l.sink.Store(nil)
		return
	}
	l.sink.Store(&sink)
}

func (l *Logger) log(level Level, event string, data map[string]interface{}) {
	if level < l.minLevel {
		return
	}

	now := time.Now()
	entry := Entry{
		Timestamp: now.UTC().Format(time.RFC3339),
		Level:     level.String(),
		Event:     event,
		Data:      data,
	}

	if data != nil {
		if s, ok := data["session"].(string); ok {
			entry.SessionID = s
			delete(data, "session")
		}
		if u, ok := data["user"].(string); ok {
			entry.UserID = u
			delete(data, "user")
		}
		if len(data) == 0 {
			entry.Data = nil
		}
	}

	// json.Marshal plus a newline is what json.Encoder.Encode wrote: the
	// stdout line does not change.
	line, err := json.Marshal(entry)
	if err == nil {
		l.output.Write(append(line, '\n'))
	}
	l.ring.Push(entry)
	if sink := l.sink.Load(); sink != nil && err == nil {
		(*sink)(now.UnixMilli(), entry, line)
	}
}

func (l *Logger) Debug(event string, data map[string]interface{}) {
	l.log(LevelDebug, event, data)
}

func (l *Logger) Info(event string, data map[string]interface{}) {
	l.log(LevelInfo, event, data)
}

func (l *Logger) Warn(event string, data map[string]interface{}) {
	l.log(LevelWarn, event, data)
}

func (l *Logger) Error(event string, data map[string]interface{}) {
	l.log(LevelError, event, data)
}

func (l *Logger) Ring() *Ring {
	return l.ring
}

// D is a shorthand for building log data maps.
type D = map[string]interface{}
