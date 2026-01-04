package wal

import (
	"bytes"
	"sync"
)

type Batch struct {
	entries []*Entry
	mu      sync.Mutex
}

func NewBatch() *Batch {
	return &Batch{
		entries: make([]*Entry, 0, 16),
	}
}

func (b *Batch) Add(entry *Entry) {
	b.mu.Lock()
	b.entries = append(b.entries, entry)
	b.mu.Unlock()
}

func (b *Batch) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}

func (b *Batch) Reset() {
	b.mu.Lock()
	b.entries = b.entries[:0]
	b.mu.Unlock()
}

func (b *Batch) Entries() []*Entry {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]*Entry, len(b.entries))
	copy(result, b.entries)
	return result
}

func (w *Writer) AppendBatch(batch *Batch) error {
	entries := batch.Entries()
	if len(entries) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, entry := range entries {
		if err := entry.Encode(&buf); err != nil {
			return err
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.buf.Write(buf.Bytes()); err != nil {
		return err
	}

	if w.sync {
		if err := w.buf.Flush(); err != nil {
			return err
		}
		return w.file.Sync()
	}

	return nil
}
