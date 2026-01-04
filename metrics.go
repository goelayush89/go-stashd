package stashd

import "sync/atomic"

type Metrics struct {
	Hits      atomic.Uint64
	Misses    atomic.Uint64
	Puts      atomic.Uint64
	Deletes   atomic.Uint64
	Evictions atomic.Uint64
	WALWrites atomic.Uint64
	WALErrors atomic.Uint64
}

type MetricsSnapshot struct {
	Hits      uint64
	Misses    uint64
	Puts      uint64
	Deletes   uint64
	Evictions uint64
	WALWrites uint64
	WALErrors uint64
	HitRate   float64
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	hits := m.Hits.Load()
	misses := m.Misses.Load()
	total := hits + misses

	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return MetricsSnapshot{
		Hits:      hits,
		Misses:    misses,
		Puts:      m.Puts.Load(),
		Deletes:   m.Deletes.Load(),
		Evictions: m.Evictions.Load(),
		WALWrites: m.WALWrites.Load(),
		WALErrors: m.WALErrors.Load(),
		HitRate:   hitRate,
	}
}

func (m *Metrics) Reset() {
	m.Hits.Store(0)
	m.Misses.Store(0)
	m.Puts.Store(0)
	m.Deletes.Store(0)
	m.Evictions.Store(0)
	m.WALWrites.Store(0)
	m.WALErrors.Store(0)
}
