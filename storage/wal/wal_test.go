package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEntry_EncodeDecodeSet(t *testing.T) {
	now := time.Now()
	entry := NewSetEntry([]byte("mykey"), []byte("myvalue"), now.Add(1*time.Hour), now)

	var buf bytes.Buffer
	if err := entry.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded := &Entry{}
	if err := decoded.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if string(decoded.Key) != "mykey" {
		t.Errorf("expected key 'mykey', got '%s'", decoded.Key)
	}
	if string(decoded.Value) != "myvalue" {
		t.Errorf("expected value 'myvalue', got '%s'", decoded.Value)
	}
	if decoded.Header.OpType != OpSet {
		t.Errorf("expected OpSet, got %d", decoded.Header.OpType)
	}
}

func TestEntry_EncodeDecodeDelete(t *testing.T) {
	now := time.Now()
	entry := NewDeleteEntry([]byte("deletedkey"), now)

	var buf bytes.Buffer
	if err := entry.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded := &Entry{}
	if err := decoded.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if string(decoded.Key) != "deletedkey" {
		t.Errorf("expected key 'deletedkey', got '%s'", decoded.Key)
	}
	if decoded.Header.OpType != OpDelete {
		t.Errorf("expected OpDelete, got %d", decoded.Header.OpType)
	}
}

func TestEntry_CRCValidation(t *testing.T) {
	now := time.Now()
	entry := NewSetEntry([]byte("key"), []byte("value"), now.Add(1*time.Hour), now)

	var buf bytes.Buffer
	entry.Encode(&buf)

	data := buf.Bytes()
	data[10] ^= 0xFF

	decoded := &Entry{}
	err := decoded.Decode(bytes.NewReader(data))
	if err == nil {
		t.Error("expected CRC error on corrupted data")
	}
}

func TestWriter_AppendAndRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	writer, err := OpenWriter(path, false)
	if err != nil {
		t.Fatalf("open writer failed: %v", err)
	}

	now := time.Now()
	entries := []*Entry{
		NewSetEntry([]byte("key1"), []byte("val1"), now.Add(1*time.Hour), now),
		NewSetEntry([]byte("key2"), []byte("val2"), now.Add(2*time.Hour), now),
		NewDeleteEntry([]byte("key1"), now),
	}

	for _, e := range entries {
		if err := writer.Append(e); err != nil {
			t.Fatalf("append failed: %v", err)
		}
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

func TestWriter_SyncWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sync.wal")

	writer, err := OpenWriter(path, true)
	if err != nil {
		t.Fatalf("open writer failed: %v", err)
	}
	defer writer.Close()

	now := time.Now()
	entry := NewSetEntry([]byte("key"), []byte("value"), now.Add(1*time.Hour), now)

	if err := writer.Append(entry); err != nil {
		t.Fatalf("append with sync failed: %v", err)
	}
}

func TestReader_NonExistentFile(t *testing.T) {
	reader, err := OpenReader("/nonexistent/path/file.wal")
	if err != nil {
		t.Fatalf("expected nil reader, got error: %v", err)
	}
	if reader != nil {
		t.Error("expected nil reader for non-existent file")
	}
}

func TestRecover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recover.wal")

	writer, _ := OpenWriter(path, false)
	now := time.Now()

	writer.Append(NewSetEntry([]byte("k1"), []byte("v1"), time.Time{}, now))
	writer.Append(NewSetEntry([]byte("k2"), []byte("v2"), time.Time{}, now))
	writer.Append(NewDeleteEntry([]byte("k1"), now))
	writer.Close()

	keys := make(map[string]bool)
	err := Recover(path, func(e *Entry) bool {
		if e.Header.OpType == OpSet {
			keys[string(e.Key)] = true
		} else if e.Header.OpType == OpDelete {
			delete(keys, string(e.Key))
		}
		return true
	})

	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}

	if len(keys) != 1 || !keys["k2"] {
		t.Errorf("expected only k2, got %v", keys)
	}
}

func TestRecover_NonExistentFile(t *testing.T) {
	err := Recover("/nonexistent/file.wal", func(e *Entry) bool {
		return true
	})
	if err != nil {
		t.Errorf("expected no error for non-existent file, got: %v", err)
	}
}

func TestWriter_FlushAndSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flush.wal")

	writer, _ := OpenWriter(path, false)
	defer writer.Close()

	now := time.Now()
	writer.Append(NewSetEntry([]byte("key"), []byte("value"), time.Time{}, now))

	if err := writer.Flush(); err != nil {
		t.Errorf("flush failed: %v", err)
	}

	if err := writer.Sync(); err != nil {
		t.Errorf("sync failed: %v", err)
	}

	info, _ := os.Stat(path)
	if info.Size() == 0 {
		t.Error("file should have content after flush")
	}
}
