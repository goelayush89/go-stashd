package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

type Writer struct {
	mu   sync.Mutex
	file *os.File
	buf  *bufio.Writer
	sync bool
}

func OpenWriter(path string, syncWrites bool) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}

	return &Writer{
		file: file,
		buf:  bufio.NewWriter(file),
		sync: syncWrites,
	}, nil
}

func (w *Writer) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := entry.Encode(w.buf); err != nil {
		return fmt.Errorf("encode entry: %w", err)
	}

	if w.sync {
		if err := w.buf.Flush(); err != nil {
			return fmt.Errorf("flush: %w", err)
		}
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
	}

	return nil
}

func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Flush()
}

func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

type Reader struct {
	file   *os.File
	reader io.Reader
}

func OpenReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open wal file: %w", err)
	}

	return &Reader{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

func (r *Reader) Next() (*Entry, error) {
	entry := &Entry{}
	if err := entry.Decode(r.reader); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	return entry, nil
}

func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

type RecoveryFunc func(entry *Entry) bool

func Recover(path string, fn RecoveryFunc) error {
	reader, err := OpenReader(path)
	if err != nil {
		return err
	}
	if reader == nil {
		return nil
	}
	defer reader.Close()

	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		if !fn(entry) {
			break
		}
	}

	return nil
}
