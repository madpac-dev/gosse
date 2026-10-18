package gosse

import (
	"context"
	"log/slog"
	"time"
)

// Default values used by [NewBroker].
const (
	DefaultHeartbeat  = 15 * time.Second
	DefaultBufferSize = 64
)

// Option configures a [Broker]. Options are passed to [NewBroker].
type Option func(*config)

type config struct {
	replay       ReplayStore
	replaySet    bool
	heartbeat    time.Duration
	bufferSize   int
	logger       *slog.Logger
	writeTimeout time.Duration
}

// WithReplay sets the store used to resend missed events to reconnecting
// clients. Pass nil to disable replay. By default the broker uses
// NewMemoryReplayStore(DefaultReplayCapacity).
func WithReplay(store ReplayStore) Option {
	return func(c *config) {
		c.replay = store
		c.replaySet = true
	}
}

// WithHeartbeat sets how often a comment line is sent on idle and busy
// connections alike, to keep proxies and load balancers from closing them.
// Zero disables heartbeats. The default is [DefaultHeartbeat].
// It panics if d is negative.
func WithHeartbeat(d time.Duration) Option {
	if d < 0 {
		panic("gosse: WithHeartbeat duration must not be negative")
	}
	return func(c *config) { c.heartbeat = d }
}

// WithBufferSize sets how many events may be queued for a single subscriber
// before it is considered too slow and disconnected. The default is
// [DefaultBufferSize]. It panics if n is less than 1.
func WithBufferSize(n int) Option {
	if n < 1 {
		panic("gosse: WithBufferSize must be at least 1")
	}
	return func(c *config) { c.bufferSize = n }
}

// WithLogger sets the logger used to report noteworthy conditions such as
// disconnected slow consumers and replay gaps. By default nothing is logged.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// discardHandler is a slog.Handler that drops everything
// (slog.DiscardHandler requires Go 1.24).
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discardHandler) WithGroup(string) slog.Handler           { return d }
