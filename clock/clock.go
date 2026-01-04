package clock

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	NewTicker(d time.Duration) Ticker
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type realClock struct{}

func Real() Clock { return &realClock{} }

func (c *realClock) Now() time.Time                   { return time.Now() }
func (c *realClock) Since(t time.Time) time.Duration  { return time.Since(t) }
func (c *realClock) NewTicker(d time.Duration) Ticker { return &realTicker{ticker: time.NewTicker(d)} }

type realTicker struct{ ticker *time.Ticker }

func (t *realTicker) C() <-chan time.Time { return t.ticker.C }
func (t *realTicker) Stop()               { t.ticker.Stop() }

// Mock provides a controllable clock for testing.
type Mock struct {
	mu      sync.RWMutex
	now     time.Time
	tickers []*mockTicker
}

func NewMock(start time.Time) *Mock { return &Mock{now: start} }

func (m *Mock) Now() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.now
}

func (m *Mock) Since(t time.Time) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.now.Sub(t)
}

func (m *Mock) Advance(d time.Duration) {
	m.mu.Lock()
	m.now = m.now.Add(d)
	now := m.now
	tickers := m.tickers
	m.mu.Unlock()

	for _, t := range tickers {
		t.maybefire(now)
	}
}

func (m *Mock) Set(t time.Time) {
	m.mu.Lock()
	m.now = t
	m.mu.Unlock()
}

func (m *Mock) NewTicker(d time.Duration) Ticker {
	m.mu.Lock()
	defer m.mu.Unlock()

	t := &mockTicker{
		interval: d,
		ch:       make(chan time.Time, 1),
		lastTick: m.now,
	}
	m.tickers = append(m.tickers, t)
	return t
}

type mockTicker struct {
	interval time.Duration
	ch       chan time.Time
	lastTick time.Time
	stopped  bool
	mu       sync.Mutex
}

func (t *mockTicker) C() <-chan time.Time { return t.ch }

func (t *mockTicker) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

func (t *mockTicker) maybefire(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped {
		return
	}

	if now.Sub(t.lastTick) >= t.interval {
		t.lastTick = now
		select {
		case t.ch <- now:
		default:
		}
	}
}
