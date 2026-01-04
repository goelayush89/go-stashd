package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

type OpType uint8

const (
	OpSet    OpType = 1
	OpDelete OpType = 2
)

type EntryHeader struct {
	CRC       uint32
	OpType    OpType
	ShardID   uint16
	KeyLen    uint32
	ValueLen  uint32
	ExpiresAt int64
	Timestamp int64
}

type Entry struct {
	Header EntryHeader
	Key    []byte
	Value  []byte
}

func (e *Entry) Encode(w io.Writer) error {
	payload := make([]byte, 0, len(e.Key)+len(e.Value))
	payload = append(payload, e.Key...)
	payload = append(payload, e.Value...)

	e.Header.CRC = crc32.ChecksumIEEE(payload)
	e.Header.KeyLen = uint32(len(e.Key))
	e.Header.ValueLen = uint32(len(e.Value))

	if err := binary.Write(w, binary.LittleEndian, e.Header.CRC); err != nil {
		return fmt.Errorf("write crc: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, e.Header.OpType); err != nil {
		return fmt.Errorf("write op: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, e.Header.ShardID); err != nil {
		return fmt.Errorf("write shardid: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, e.Header.KeyLen); err != nil {
		return fmt.Errorf("write keylen: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, e.Header.ValueLen); err != nil {
		return fmt.Errorf("write valuelen: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, e.Header.ExpiresAt); err != nil {
		return fmt.Errorf("write expires: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, e.Header.Timestamp); err != nil {
		return fmt.Errorf("write timestamp: %w", err)
	}

	if _, err := w.Write(e.Key); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	if _, err := w.Write(e.Value); err != nil {
		return fmt.Errorf("write value: %w", err)
	}

	return nil
}

func (e *Entry) Decode(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &e.Header.CRC); err != nil {
		return fmt.Errorf("read crc: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Header.OpType); err != nil {
		return fmt.Errorf("read op: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Header.ShardID); err != nil {
		return fmt.Errorf("read shardid: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Header.KeyLen); err != nil {
		return fmt.Errorf("read keylen: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Header.ValueLen); err != nil {
		return fmt.Errorf("read valuelen: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Header.ExpiresAt); err != nil {
		return fmt.Errorf("read expires: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Header.Timestamp); err != nil {
		return fmt.Errorf("read timestamp: %w", err)
	}

	e.Key = make([]byte, e.Header.KeyLen)
	if _, err := io.ReadFull(r, e.Key); err != nil {
		return fmt.Errorf("read key: %w", err)
	}
	e.Value = make([]byte, e.Header.ValueLen)
	if _, err := io.ReadFull(r, e.Value); err != nil {
		return fmt.Errorf("read value: %w", err)
	}

	payload := make([]byte, 0, len(e.Key)+len(e.Value))
	payload = append(payload, e.Key...)
	payload = append(payload, e.Value...)
	if crc32.ChecksumIEEE(payload) != e.Header.CRC {
		return fmt.Errorf("crc mismatch: data corrupted")
	}

	return nil
}

func NewSetEntry(key, value []byte, expiresAt time.Time, timestamp time.Time) *Entry {
	e := &Entry{
		Header: EntryHeader{
			OpType:    OpSet,
			Timestamp: timestamp.UnixNano(),
		},
		Key:   key,
		Value: value,
	}
	if !expiresAt.IsZero() {
		e.Header.ExpiresAt = expiresAt.UnixNano()
	}
	return e
}

func NewDeleteEntry(key []byte, timestamp time.Time) *Entry {
	return &Entry{
		Header: EntryHeader{
			OpType:    OpDelete,
			Timestamp: timestamp.UnixNano(),
		},
		Key: key,
	}
}

func NewSetEntryWithShard(key, value []byte, expiresAt time.Time, timestamp time.Time, shardID uint16) *Entry {
	e := NewSetEntry(key, value, expiresAt, timestamp)
	e.Header.ShardID = shardID
	return e
}

func NewDeleteEntryWithShard(key []byte, timestamp time.Time, shardID uint16) *Entry {
	e := NewDeleteEntry(key, timestamp)
	e.Header.ShardID = shardID
	return e
}
