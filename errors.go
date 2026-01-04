package stashd

import "errors"

var (
	ErrClosed        = errors.New("stash is closed")
	ErrKeyNotFound   = errors.New("key not found")
	ErrCodecRequired = errors.New("key and value codecs required for WAL persistence")
	ErrInvalidConfig = errors.New("invalid configuration")
)
