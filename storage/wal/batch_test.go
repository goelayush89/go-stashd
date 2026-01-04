package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBatch_AddAndLen(t *testing.T) {
	batch := NewBatch()

	if batch.Len() != 0 {
		t.Errorf("expected empty batch, got len %d", batch.Len())
	}

	now := time.Now()
	batch.Add(NewSetEntry([]byte("key1"), []byte("val1"), now.Add(time.Hour), now))
	batch.Add(NewSetEntry([]byte("key2"), []byte("val2"), now.Add(time.Hour), now))

	if batch.Len() != 2 {
		t.Errorf("expected len 2, got %d", batch.Len())
	}
}

func TestBatch_Reset(t *testing.T) {
	batch := NewBatch()
	now := time.Now()
	batch.Add(NewSetEntry([]byte("key1"), []byte("val1"), time.Time{}, now))
	batch.Reset()

	if batch.Len() != 0 {
		t.Errorf("expected empty batch after reset, got len %d", batch.Len())
	}
}

func TestBatch_Entries(t *testing.T) {
	batch := NewBatch()
	now := time.Now()
	batch.Add(NewSetEntry([]byte("key1"), []byte("val1"), time.Time{}, now))
	batch.Add(NewDeleteEntry([]byte("key2"), now))

	entries := batch.Entries()
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
}

func TestWriter_AppendBatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batch.wal")

	writer, err := OpenWriter(path, false)
	if err != nil {
		t.Fatalf("open writer failed: %v", err)
	}

	batch := NewBatch()
	now := time.Now()
	batch.Add(NewSetEntry([]byte("key1"), []byte("val1"), now.Add(time.Hour), now))
	batch.Add(NewSetEntry([]byte("key2"), []byte("val2"), now.Add(time.Hour), now))
	batch.Add(NewDeleteEntry([]byte("key1"), now))

	if err := writer.AppendBatch(batch); err != nil {
		t.Fatalf("append batch failed: %v", err)
	}

	writer.Close()

	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("open reader failed: %v", err)
	}
	defer reader.Close()

	count := 0
	for {
		_, err := reader.Next()
		if err != nil {
			break
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 entries, got %d", count)
	}
}

func TestWriter_AppendBatch_Empty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.wal")

	writer, err := OpenWriter(path, false)
	if err != nil {
		t.Fatalf("open writer failed: %v", err)
	}
	defer writer.Close()

	batch := NewBatch()
	if err := writer.AppendBatch(batch); err != nil {
		t.Fatalf("append empty batch should not fail: %v", err)
	}

	info, _ := os.Stat(path)
	if info.Size() != 0 {
		t.Error("file should be empty for empty batch")
	}
}

func TestWriter_AppendBatch_WithSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sync.wal")

	writer, err := OpenWriter(path, true)
	if err != nil {
		t.Fatalf("open writer failed: %v", err)
	}
	defer writer.Close()

	batch := NewBatch()
	now := time.Now()
	batch.Add(NewSetEntry([]byte("key1"), []byte("val1"), time.Time{}, now))

	if err := writer.AppendBatch(batch); err != nil {
		t.Fatalf("append batch with sync failed: %v", err)
	}
}

func TestEntry_ShardID(t *testing.T) {
	now := time.Now()

	entry := NewSetEntryWithShard([]byte("key"), []byte("val"), now.Add(time.Hour), now, 42)
	if entry.Header.ShardID != 42 {
		t.Errorf("expected shard ID 42, got %d", entry.Header.ShardID)
	}

	deleteEntry := NewDeleteEntryWithShard([]byte("key"), now, 99)
	if deleteEntry.Header.ShardID != 99 {
		t.Errorf("expected shard ID 99, got %d", deleteEntry.Header.ShardID)
	}
}
