package stashd

import (
	"container/list"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/goelayush89/go-stashd/clock"
	"github.com/goelayush89/go-stashd/eviction"
	"github.com/goelayush89/go-stashd/storage"
	"github.com/goelayush89/go-stashd/storage/wal"
)

type Stash[K comparable, V any] struct {
	config     Config
	backend    *storage.Memory[K, stashEntry[V]]
	eviction   eviction.Policy[K]
	clock      clock.Clock
	wal        *wal.Writer
	keyCodec   KeyCodec[K]
	valCodec   ValueCodec[V]
	metrics    *Metrics
	loader     Loader[K, V]
	events     EventHandlers[K, V]
	lruOrder   *list.List
	lruIndex   map[K]*list.Element
	refreshing map[K]bool
	shardID    uint16
	ownsWAL    bool

	mu        sync.RWMutex
	closed    bool
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

type stashEntry[V any] struct {
	Value     V
	ExpiresAt time.Time
	RefreshAt time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

type KeyCodec[K any] interface {
	Encode(key K) ([]byte, error)
	Decode(data []byte) (K, error)
}

type ValueCodec[V any] interface {
	Encode(value V) ([]byte, error)
	Decode(data []byte) (V, error)
}

type StringCodec struct{}

func (StringCodec) Encode(s string) ([]byte, error)    { return []byte(s), nil }
func (StringCodec) Decode(data []byte) (string, error) { return string(data), nil }

type JSONCodec[T any] struct{}

func (JSONCodec[T]) Encode(v T) ([]byte, error) { return json.Marshal(v) }
func (JSONCodec[T]) Decode(data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}

type Option[K comparable, V any] func(*Stash[K, V])

func WithClock[K comparable, V any](c clock.Clock) Option[K, V] {
	return func(s *Stash[K, V]) { s.clock = c }
}

func WithKeyCodec[K comparable, V any](codec KeyCodec[K]) Option[K, V] {
	return func(s *Stash[K, V]) { s.keyCodec = codec }
}

func WithValueCodec[K comparable, V any](codec ValueCodec[V]) Option[K, V] {
	return func(s *Stash[K, V]) { s.valCodec = codec }
}

func WithLoader[K comparable, V any](loader Loader[K, V]) Option[K, V] {
	return func(s *Stash[K, V]) { s.loader = loader }
}

func WithEventHandlers[K comparable, V any](handlers EventHandlers[K, V]) Option[K, V] {
	return func(s *Stash[K, V]) { s.events = handlers }
}

func WithExternalWAL[K comparable, V any](w *wal.Writer, shardID uint16) Option[K, V] {
	return func(s *Stash[K, V]) {
		s.wal = w
		s.shardID = shardID
		s.ownsWAL = false
	}
}

func Open[K comparable, V any](cfg Config, opts ...Option[K, V]) (*Stash[K, V], error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	var evictionPolicy eviction.Policy[K]
	if cfg.EvictionPolicy == EvictionPolicyTinyLFU {
		evictionPolicy = eviction.NewTinyLFU[K](cfg.MaxEntries)
	} else {
		evictionPolicy = eviction.NewTTL[K]()
	}

	s := &Stash[K, V]{
		config:     cfg,
		backend:    storage.NewMemory[K, stashEntry[V]](),
		eviction:   evictionPolicy,
		clock:      clock.Real(),
		metrics:    &Metrics{},
		lruOrder:   list.New(),
		lruIndex:   make(map[K]*list.Element),
		refreshing: make(map[K]bool),
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	if cfg.WALPath != "" && s.wal == nil {
		if s.keyCodec == nil || s.valCodec == nil {
			return nil, ErrCodecRequired
		}

		if err := ensureDir(cfg.WALPath); err != nil {
			return nil, fmt.Errorf("create wal directory: %w", err)
		}

		if err := s.recover(); err != nil {
			return nil, fmt.Errorf("wal recovery: %w", err)
		}

		w, err := wal.OpenWriter(cfg.WALPath, cfg.SyncWrites)
		if err != nil {
			return nil, fmt.Errorf("open wal writer: %w", err)
		}
		s.wal = w
		s.ownsWAL = true
	}

	if cfg.CleanupInterval > 0 {
		go s.cleanupLoop()
	}

	return s, nil
}

func ensureDir(path string) error {
	dir := filepath.Dir(path)
	info, err := os.Stat(path)
	if err == nil && info.IsDir() {
		return fmt.Errorf("WAL path cannot be a directory: %s", path)
	}
	return os.MkdirAll(dir, 0755)
}

func (s *Stash[K, V]) recover() error {
	now := s.clock.Now()

	return wal.Recover(s.config.WALPath, func(entry *wal.Entry) bool {
		key, err := s.keyCodec.Decode(entry.Key)
		if err != nil {
			return true
		}

		switch entry.Header.OpType {
		case wal.OpSet:
			var expiresAt time.Time
			if entry.Header.ExpiresAt != 0 {
				expiresAt = time.Unix(0, entry.Header.ExpiresAt)
			}

			if !expiresAt.IsZero() && now.After(expiresAt) {
				return true
			}

			value, err := s.valCodec.Decode(entry.Value)
			if err != nil {
				return true
			}

			se := stashEntry[V]{
				Value:     value,
				ExpiresAt: expiresAt,
				CreatedAt: time.Unix(0, entry.Header.Timestamp),
				UpdatedAt: time.Unix(0, entry.Header.Timestamp),
			}

			_, exists := s.backend.Load(key)
			s.backend.Store(key, se)

			if exists {
				s.eviction.OnUpdate(key, expiresAt)
				if elem, ok := s.lruIndex[key]; ok {
					s.lruOrder.MoveToFront(elem)
				}
			} else {
				s.eviction.OnInsert(key, expiresAt)
				elem := s.lruOrder.PushFront(key)
				s.lruIndex[key] = elem
			}

		case wal.OpDelete:
			s.backend.Delete(key)
			s.eviction.OnDelete(key)
			if elem, ok := s.lruIndex[key]; ok {
				s.lruOrder.Remove(elem)
				delete(s.lruIndex, key)
			}
		}

		return true
	})
}

func (s *Stash[K, V]) cleanupLoop() {
	defer close(s.stoppedCh)

	ticker := s.clock.NewTicker(s.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C():
			s.DeleteExpired()
		}
	}
}

func (s *Stash[K, V]) Put(key K, value V) error {
	return s.PutWithTTL(key, value, s.config.DefaultTTL)
}

func (s *Stash[K, V]) PutWithTTL(key K, value V, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	now := s.clock.Now()
	var expiresAt time.Time
	var refreshAt time.Time
	if ttl > 0 {
		expiresAt = now.Add(ttl)
		if s.config.RefreshAfter > 0 {
			refreshAt = now.Add(s.config.RefreshAfter)
		}
	}

	oldEntry, exists := s.backend.Load(key)

	se := stashEntry[V]{
		Value:     value,
		ExpiresAt: expiresAt,
		RefreshAt: refreshAt,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if exists {
		se.CreatedAt = oldEntry.CreatedAt
	}

	if s.wal != nil {
		keyBytes, err := s.keyCodec.Encode(key)
		if err != nil {
			return fmt.Errorf("encode key: %w", err)
		}
		valBytes, err := s.valCodec.Encode(value)
		if err != nil {
			return fmt.Errorf("encode value: %w", err)
		}
		walEntry := wal.NewSetEntryWithShard(keyBytes, valBytes, expiresAt, now, s.shardID)
		if err := s.wal.Append(walEntry); err != nil {
			s.metrics.WALErrors.Add(1)
			return fmt.Errorf("wal append: %w", err)
		}
		s.metrics.WALWrites.Add(1)
	}

	s.backend.Store(key, se)
	s.metrics.Puts.Add(1)

	if exists && (oldEntry.ExpiresAt.IsZero() || now.Before(oldEntry.ExpiresAt)) {
		s.eviction.OnUpdate(key, expiresAt)
		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.MoveToFront(elem)
		}
		if s.events.OnUpdate != nil {
			oldVal := oldEntry.Value
			s.events.OnUpdate(key, oldVal, value)
		}
	} else {
		if exists {
			s.eviction.OnDelete(key)
			if elem, ok := s.lruIndex[key]; ok {
				s.lruOrder.Remove(elem)
				delete(s.lruIndex, key)
			}
		}
		s.eviction.OnInsert(key, expiresAt)
		elem := s.lruOrder.PushFront(key)
		s.lruIndex[key] = elem
		if s.events.OnPut != nil {
			s.events.OnPut(key, value)
		}
	}

	if s.config.MaxEntries > 0 && s.backend.Len() > s.config.MaxEntries {
		s.evict()
	}

	return nil
}

func (s *Stash[K, V]) evict() {
	for s.backend.Len() > s.config.MaxEntries {
		key, ok := s.eviction.Victim()
		if !ok {
			elem := s.lruOrder.Back()
			if elem == nil {
				break
			}
			key = elem.Value.(K)
		}

		var evictedValue V
		if se, found := s.backend.Load(key); found {
			evictedValue = se.Value
		}

		if s.wal != nil && s.keyCodec != nil {
			keyBytes, err := s.keyCodec.Encode(key)
			if err == nil {
				walEntry := wal.NewDeleteEntryWithShard(keyBytes, s.clock.Now(), s.shardID)
				if err := s.wal.Append(walEntry); err != nil {
					s.metrics.WALErrors.Add(1)
				} else {
					s.metrics.WALWrites.Add(1)
				}
			}
		}

		s.backend.Delete(key)
		s.eviction.OnDelete(key)
		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.Remove(elem)
			delete(s.lruIndex, key)
		}
		s.metrics.Evictions.Add(1)
		if s.events.OnEviction != nil {
			s.events.OnEviction(EvictionReasonCapacity, key, evictedValue)
		}
	}
}

func (s *Stash[K, V]) Fetch(key K) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var zero V
	if s.closed {
		return zero, false
	}

	se, ok := s.backend.Load(key)
	if !ok {
		s.metrics.Misses.Add(1)
		return zero, false
	}

	now := s.clock.Now()
	if se.ExpiresAt.IsZero() || now.Before(se.ExpiresAt) {
		s.eviction.OnAccess(key)
		s.metrics.Hits.Add(1)
		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.MoveToFront(elem)
		}
		if s.events.OnHit != nil {
			s.events.OnHit(key, se.Value)
		}

		if s.shouldRefresh(key, se, now) {
			s.triggerRefresh(key)
		}

		return se.Value, true
	}

	s.metrics.Misses.Add(1)
	return zero, false
}

func (s *Stash[K, V]) shouldRefresh(key K, se stashEntry[V], now time.Time) bool {
	if s.loader == nil {
		return false
	}
	if se.RefreshAt.IsZero() {
		return false
	}
	if now.Before(se.RefreshAt) {
		return false
	}
	if s.refreshing[key] {
		return false
	}
	return true
}

func (s *Stash[K, V]) triggerRefresh(key K) {
	s.refreshing[key] = true

	go func() {
		val, err := s.loader.Load(key)

		s.mu.Lock()
		defer s.mu.Unlock()

		delete(s.refreshing, key)

		if err != nil {
			return
		}
		if s.closed {
			return
		}

		se, ok := s.backend.Load(key)
		if !ok {
			return
		}

		now := s.clock.Now()
		var expiresAt time.Time
		var refreshAt time.Time
		if s.config.DefaultTTL > 0 {
			expiresAt = now.Add(s.config.DefaultTTL)
			if s.config.RefreshAfter > 0 {
				refreshAt = now.Add(s.config.RefreshAfter)
			}
		}

		newEntry := stashEntry[V]{
			Value:     val,
			ExpiresAt: expiresAt,
			RefreshAt: refreshAt,
			CreatedAt: se.CreatedAt,
			UpdatedAt: now,
		}

		if s.wal != nil {
			keyBytes, err := s.keyCodec.Encode(key)
			if err == nil {
				valBytes, err := s.valCodec.Encode(val)
				if err == nil {
					walEntry := wal.NewSetEntryWithShard(keyBytes, valBytes, expiresAt, now, s.shardID)
					if err := s.wal.Append(walEntry); err != nil {
						s.metrics.WALErrors.Add(1)
					} else {
						s.metrics.WALWrites.Add(1)
					}
				}
			}
		}

		s.backend.Store(key, newEntry)
		s.eviction.OnUpdate(key, expiresAt)
		s.metrics.Puts.Add(1)

		if s.events.OnUpdate != nil {
			s.events.OnUpdate(key, se.Value, val)
		}
	}()
}

func (s *Stash[K, V]) Has(key K) bool {
	_, ok := s.Fetch(key)
	return ok
}

func (s *Stash[K, V]) Remove(key K) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false, ErrClosed
	}

	se, exists := s.backend.Load(key)
	if !exists {
		return false, nil
	}

	if s.wal != nil {
		keyBytes, err := s.keyCodec.Encode(key)
		if err != nil {
			return false, fmt.Errorf("encode key: %w", err)
		}
		walEntry := wal.NewDeleteEntryWithShard(keyBytes, s.clock.Now(), s.shardID)
		if err := s.wal.Append(walEntry); err != nil {
			s.metrics.WALErrors.Add(1)
			return false, fmt.Errorf("wal append: %w", err)
		}
		s.metrics.WALWrites.Add(1)
	}

	s.backend.Delete(key)
	s.eviction.OnDelete(key)
	s.metrics.Deletes.Add(1)

	if elem, ok := s.lruIndex[key]; ok {
		s.lruOrder.Remove(elem)
		delete(s.lruIndex, key)
	}

	if s.events.OnEviction != nil {
		s.events.OnEviction(EvictionReasonDeleted, key, se.Value)
	}

	return true, nil
}

func (s *Stash[K, V]) GetAndDelete(key K) (V, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var zero V
	if s.closed {
		return zero, false, ErrClosed
	}

	se, exists := s.backend.Load(key)
	if !exists {
		return zero, false, nil
	}

	if !se.ExpiresAt.IsZero() && s.clock.Now().After(se.ExpiresAt) {
		return zero, false, nil
	}

	if s.wal != nil {
		keyBytes, err := s.keyCodec.Encode(key)
		if err != nil {
			return zero, false, fmt.Errorf("encode key: %w", err)
		}
		walEntry := wal.NewDeleteEntryWithShard(keyBytes, s.clock.Now(), s.shardID)
		if err := s.wal.Append(walEntry); err != nil {
			s.metrics.WALErrors.Add(1)
			return zero, false, fmt.Errorf("wal append: %w", err)
		}
		s.metrics.WALWrites.Add(1)
	}

	s.backend.Delete(key)
	s.eviction.OnDelete(key)
	s.metrics.Deletes.Add(1)

	if elem, ok := s.lruIndex[key]; ok {
		s.lruOrder.Remove(elem)
		delete(s.lruIndex, key)
	}

	if s.events.OnEviction != nil {
		s.events.OnEviction(EvictionReasonDeleted, key, se.Value)
	}

	return se.Value, true, nil
}

func (s *Stash[K, V]) SetIfAbsent(key K, value V) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false, ErrClosed
	}

	se, exists := s.backend.Load(key)
	now := s.clock.Now()

	if exists && (se.ExpiresAt.IsZero() || now.Before(se.ExpiresAt)) {
		return false, nil
	}

	if exists {
		s.eviction.OnDelete(key)
		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.Remove(elem)
			delete(s.lruIndex, key)
		}
	}

	var expiresAt time.Time
	if s.config.DefaultTTL > 0 {
		expiresAt = now.Add(s.config.DefaultTTL)
	}

	newEntry := stashEntry[V]{
		Value:     value,
		ExpiresAt: expiresAt,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if s.wal != nil {
		keyBytes, err := s.keyCodec.Encode(key)
		if err != nil {
			return false, fmt.Errorf("encode key: %w", err)
		}
		valBytes, err := s.valCodec.Encode(value)
		if err != nil {
			return false, fmt.Errorf("encode value: %w", err)
		}
		walEntry := wal.NewSetEntryWithShard(keyBytes, valBytes, expiresAt, now, s.shardID)
		if err := s.wal.Append(walEntry); err != nil {
			s.metrics.WALErrors.Add(1)
			return false, fmt.Errorf("wal append: %w", err)
		}
		s.metrics.WALWrites.Add(1)
	}

	s.backend.Store(key, newEntry)
	s.eviction.OnInsert(key, expiresAt)
	s.metrics.Puts.Add(1)

	elem := s.lruOrder.PushFront(key)
	s.lruIndex[key] = elem

	if s.events.OnPut != nil {
		s.events.OnPut(key, value)
	}

	if s.config.MaxEntries > 0 && s.backend.Len() > s.config.MaxEntries {
		s.evict()
	}

	return true, nil
}

func (s *Stash[K, V]) SetIfPresent(key K, value V) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false, ErrClosed
	}

	se, exists := s.backend.Load(key)
	now := s.clock.Now()

	if !exists || (!se.ExpiresAt.IsZero() && now.After(se.ExpiresAt)) {
		return false, nil
	}

	var expiresAt time.Time
	if s.config.DefaultTTL > 0 {
		expiresAt = now.Add(s.config.DefaultTTL)
	}

	newEntry := stashEntry[V]{
		Value:     value,
		ExpiresAt: expiresAt,
		CreatedAt: se.CreatedAt,
		UpdatedAt: now,
	}

	if s.wal != nil {
		keyBytes, err := s.keyCodec.Encode(key)
		if err != nil {
			return false, fmt.Errorf("encode key: %w", err)
		}
		valBytes, err := s.valCodec.Encode(value)
		if err != nil {
			return false, fmt.Errorf("encode value: %w", err)
		}
		walEntry := wal.NewSetEntryWithShard(keyBytes, valBytes, expiresAt, now, s.shardID)
		if err := s.wal.Append(walEntry); err != nil {
			s.metrics.WALErrors.Add(1)
			return false, fmt.Errorf("wal append: %w", err)
		}
		s.metrics.WALWrites.Add(1)
	}

	s.backend.Store(key, newEntry)
	s.eviction.OnUpdate(key, expiresAt)
	s.metrics.Puts.Add(1)

	if elem, ok := s.lruIndex[key]; ok {
		s.lruOrder.MoveToFront(elem)
	}

	if s.events.OnUpdate != nil {
		s.events.OnUpdate(key, se.Value, value)
	}

	return true, nil
}

func (s *Stash[K, V]) DeleteFunc(fn func(key K, value V) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0
	}

	var toDelete []K
	now := s.clock.Now()

	s.backend.Range(func(k K, v stashEntry[V]) bool {
		if v.ExpiresAt.IsZero() || now.Before(v.ExpiresAt) {
			if fn(k, v.Value) {
				toDelete = append(toDelete, k)
			}
		}
		return true
	})

	deleted := 0
	for _, key := range toDelete {
		if s.wal != nil {
			keyBytes, err := s.keyCodec.Encode(key)
			if err != nil {
				continue
			}
			walEntry := wal.NewDeleteEntryWithShard(keyBytes, now, s.shardID)
			if err := s.wal.Append(walEntry); err != nil {
				s.metrics.WALErrors.Add(1)
				continue
			}
			s.metrics.WALWrites.Add(1)
		}

		s.backend.Delete(key)
		s.eviction.OnDelete(key)

		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.Remove(elem)
			delete(s.lruIndex, key)
		}
		deleted++
	}

	s.metrics.Deletes.Add(uint64(deleted))
	return deleted
}

func (s *Stash[K, V]) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	const entryOverhead = 128
	return int64(s.backend.Len()) * entryOverhead
}

func (s *Stash[K, V]) DeleteExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0
	}

	count := 0
	now := s.clock.Now()

	for {
		key, _, ok := s.eviction.NextExpired(now)
		if !ok {
			break
		}
		if se, loaded := s.backend.Load(key); loaded {
			if s.events.OnEviction != nil {
				s.events.OnEviction(EvictionReasonExpired, key, se.Value)
			}
		}
		s.backend.Delete(key)
		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.Remove(elem)
			delete(s.lruIndex, key)
		}
		count++
	}

	s.metrics.Evictions.Add(uint64(count))
	return count
}

func (s *Stash[K, V]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.backend.Len()
}

func (s *Stash[K, V]) Purge() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	s.backend.Clear()
	s.eviction.Clear()
	s.lruOrder.Init()
	s.lruIndex = make(map[K]*list.Element)

	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			return fmt.Errorf("close wal: %w", err)
		}
		if err := os.Truncate(s.config.WALPath, 0); err != nil {
			return fmt.Errorf("truncate wal: %w", err)
		}
		w, err := wal.OpenWriter(s.config.WALPath, s.config.SyncWrites)
		if err != nil {
			return fmt.Errorf("reopen wal: %w", err)
		}
		s.wal = w
	}

	return nil
}

func (s *Stash[K, V]) GetOrLoad(key K) (V, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var zero V
	if s.closed {
		return zero, ErrClosed
	}

	se, ok := s.backend.Load(key)
	if ok && (se.ExpiresAt.IsZero() || s.clock.Now().Before(se.ExpiresAt)) {
		s.eviction.OnAccess(key)
		s.metrics.Hits.Add(1)
		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.MoveToFront(elem)
		}
		return se.Value, nil
	}

	if s.loader == nil {
		s.metrics.Misses.Add(1)
		return zero, ErrKeyNotFound
	}

	s.mu.Unlock()
	val, err := s.loader.Load(key)
	s.mu.Lock()

	if err != nil {
		s.metrics.Misses.Add(1)
		return zero, err
	}

	if s.closed {
		return zero, ErrClosed
	}

	se2, ok2 := s.backend.Load(key)
	if ok2 && (se2.ExpiresAt.IsZero() || s.clock.Now().Before(se2.ExpiresAt)) {
		s.metrics.Hits.Add(1)
		return se2.Value, nil
	}

	now := s.clock.Now()
	var expiresAt time.Time
	if s.config.DefaultTTL > 0 {
		expiresAt = now.Add(s.config.DefaultTTL)
	}

	newEntry := stashEntry[V]{
		Value:     val,
		ExpiresAt: expiresAt,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if s.wal != nil {
		keyBytes, err := s.keyCodec.Encode(key)
		if err == nil {
			valBytes, err := s.valCodec.Encode(val)
			if err == nil {
				walEntry := wal.NewSetEntryWithShard(keyBytes, valBytes, expiresAt, now, s.shardID)
				if err := s.wal.Append(walEntry); err != nil {
					s.metrics.WALErrors.Add(1)
				} else {
					s.metrics.WALWrites.Add(1)
				}
			}
		}
	}

	exists := ok2
	if exists {
		s.eviction.OnDelete(key)
		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.Remove(elem)
			delete(s.lruIndex, key)
		}
	}

	s.backend.Store(key, newEntry)
	s.eviction.OnInsert(key, expiresAt)
	s.metrics.Puts.Add(1)

	elem := s.lruOrder.PushFront(key)
	s.lruIndex[key] = elem

	if s.events.OnPut != nil {
		s.events.OnPut(key, val)
	}

	if s.config.MaxEntries > 0 && s.backend.Len() > s.config.MaxEntries {
		s.evict()
	}

	return val, nil
}

func (s *Stash[K, V]) GetOrSet(key K, value V) (V, bool) {
	return s.GetOrSetFunc(key, func() V { return value })
}

func (s *Stash[K, V]) GetOrSetFunc(key K, fn func() V) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var zero V
	if s.closed {
		return zero, false
	}

	se, ok := s.backend.Load(key)
	if ok && (se.ExpiresAt.IsZero() || s.clock.Now().Before(se.ExpiresAt)) {
		s.eviction.OnAccess(key)
		s.metrics.Hits.Add(1)
		return se.Value, true
	}

	if ok {
		s.eviction.OnDelete(key)
		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.Remove(elem)
			delete(s.lruIndex, key)
		}
	}

	value := fn()
	now := s.clock.Now()
	var expiresAt time.Time
	if s.config.DefaultTTL > 0 {
		expiresAt = now.Add(s.config.DefaultTTL)
	}

	newEntry := stashEntry[V]{
		Value:     value,
		ExpiresAt: expiresAt,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if s.wal != nil {
		keyBytes, err := s.keyCodec.Encode(key)
		if err == nil {
			valBytes, err := s.valCodec.Encode(value)
			if err == nil {
				walEntry := wal.NewSetEntryWithShard(keyBytes, valBytes, expiresAt, now, s.shardID)
				if err := s.wal.Append(walEntry); err != nil {
					s.metrics.WALErrors.Add(1)
				} else {
					s.metrics.WALWrites.Add(1)
				}
			}
		}
	}

	s.backend.Store(key, newEntry)
	s.eviction.OnInsert(key, expiresAt)
	s.metrics.Puts.Add(1)

	elem := s.lruOrder.PushFront(key)
	s.lruIndex[key] = elem

	if s.events.OnPut != nil {
		s.events.OnPut(key, value)
	}

	if s.config.MaxEntries > 0 && s.backend.Len() > s.config.MaxEntries {
		s.evict()
	}

	s.metrics.Misses.Add(1)
	return value, false
}

func (s *Stash[K, V]) Touch(key K) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	se, ok := s.backend.Load(key)
	if !ok {
		return false
	}

	if !se.ExpiresAt.IsZero() && s.clock.Now().After(se.ExpiresAt) {
		return false
	}

	if elem, ok := s.lruIndex[key]; ok {
		s.lruOrder.MoveToFront(elem)
	}

	if s.config.DefaultTTL > 0 {
		now := s.clock.Now()
		se.ExpiresAt = now.Add(s.config.DefaultTTL)
		if s.config.RefreshAfter > 0 {
			se.RefreshAt = now.Add(s.config.RefreshAfter)
		}
		se.UpdatedAt = now
		s.backend.Store(key, se)
		s.eviction.OnUpdate(key, se.ExpiresAt)

		if s.wal != nil {
			keyBytes, err := s.keyCodec.Encode(key)
			if err == nil {
				valBytes, err := s.valCodec.Encode(se.Value)
				if err == nil {
					walEntry := wal.NewSetEntryWithShard(keyBytes, valBytes, se.ExpiresAt, now, s.shardID)
					if err := s.wal.Append(walEntry); err != nil {
						s.metrics.WALErrors.Add(1)
					} else {
						s.metrics.WALWrites.Add(1)
					}
				}
			}
		}
	}

	return true
}

func (s *Stash[K, V]) Keys() []K {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]K, 0, s.backend.Len())
	now := s.clock.Now()

	s.backend.Range(func(k K, v stashEntry[V]) bool {
		if v.ExpiresAt.IsZero() || now.Before(v.ExpiresAt) {
			keys = append(keys, k)
		}
		return true
	})

	return keys
}

func (s *Stash[K, V]) Range(fn func(key K, value V) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.clock.Now()

	s.backend.Range(func(k K, v stashEntry[V]) bool {
		if v.ExpiresAt.IsZero() || now.Before(v.ExpiresAt) {
			return fn(k, v.Value)
		}
		return true
	})
}

func (s *Stash[K, V]) Metrics() MetricsSnapshot {
	return s.metrics.Snapshot()
}

func (s *Stash[K, V]) ResetMetrics() {
	s.metrics.Reset()
}

func (s *Stash[K, V]) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	hasCleanup := s.config.CleanupInterval > 0
	s.mu.Unlock()

	close(s.stopCh)
	if hasCleanup {
		<-s.stoppedCh
	}

	if s.wal != nil && s.ownsWAL {
		if err := s.wal.Close(); err != nil {
			return fmt.Errorf("close wal: %w", err)
		}
	}

	return s.backend.Close()
}

type Item[V any] struct {
	Value     V
	ExpiresAt time.Time
	TTL       time.Duration
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (s *Stash[K, V]) FetchWithTTL(key K) (Item[V], bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var zero Item[V]
	if s.closed {
		return zero, false
	}

	se, ok := s.backend.Load(key)
	if !ok {
		s.metrics.Misses.Add(1)
		return zero, false
	}

	now := s.clock.Now()
	if !se.ExpiresAt.IsZero() && now.After(se.ExpiresAt) {
		s.metrics.Misses.Add(1)
		return zero, false
	}

	s.eviction.OnAccess(key)
	s.metrics.Hits.Add(1)

	if elem, ok := s.lruIndex[key]; ok {
		s.lruOrder.MoveToFront(elem)
	}

	if s.events.OnHit != nil {
		s.events.OnHit(key, se.Value)
	}

	if s.shouldRefresh(key, se, now) {
		s.triggerRefresh(key)
	}

	var ttl time.Duration
	if !se.ExpiresAt.IsZero() {
		ttl = se.ExpiresAt.Sub(now)
		if ttl < 0 {
			ttl = 0
		}
	}

	return Item[V]{
		Value:     se.Value,
		ExpiresAt: se.ExpiresAt,
		TTL:       ttl,
		CreatedAt: se.CreatedAt,
		UpdatedAt: se.UpdatedAt,
	}, true
}

func (s *Stash[K, V]) GetOrLoadWithContext(ctx context.Context, key K) (V, error) {
	s.mu.Lock()

	var zero V
	if s.closed {
		s.mu.Unlock()
		return zero, ErrClosed
	}

	se, ok := s.backend.Load(key)
	if ok && (se.ExpiresAt.IsZero() || s.clock.Now().Before(se.ExpiresAt)) {
		s.eviction.OnAccess(key)
		s.metrics.Hits.Add(1)
		if elem, ok := s.lruIndex[key]; ok {
			s.lruOrder.MoveToFront(elem)
		}
		s.mu.Unlock()
		return se.Value, nil
	}

	if s.loader == nil {
		s.metrics.Misses.Add(1)
		s.mu.Unlock()
		return zero, ErrKeyNotFound
	}

	s.mu.Unlock()

	type result struct {
		val V
		err error
	}
	ch := make(chan result, 1)

	go func() {
		val, err := s.loader.Load(key)
		ch <- result{val, err}
	}()

	select {
	case <-ctx.Done():
		s.metrics.Misses.Add(1)
		return zero, ctx.Err()
	case res := <-ch:
		if res.err != nil {
			s.metrics.Misses.Add(1)
			return zero, res.err
		}
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.closed {
			return zero, ErrClosed
		}

		se2, ok2 := s.backend.Load(key)
		if ok2 && (se2.ExpiresAt.IsZero() || s.clock.Now().Before(se2.ExpiresAt)) {
			s.metrics.Hits.Add(1)
			return se2.Value, nil
		}

		now := s.clock.Now()
		var expiresAt time.Time
		if s.config.DefaultTTL > 0 {
			expiresAt = now.Add(s.config.DefaultTTL)
		}

		newEntry := stashEntry[V]{
			Value:     res.val,
			ExpiresAt: expiresAt,
			CreatedAt: now,
			UpdatedAt: now,
		}

		if s.wal != nil {
			keyBytes, err := s.keyCodec.Encode(key)
			if err == nil {
				valBytes, err := s.valCodec.Encode(res.val)
				if err == nil {
					walEntry := wal.NewSetEntryWithShard(keyBytes, valBytes, expiresAt, now, s.shardID)
					if err := s.wal.Append(walEntry); err != nil {
						s.metrics.WALErrors.Add(1)
					} else {
						s.metrics.WALWrites.Add(1)
					}
				}
			}
		}

		if ok2 {
			s.eviction.OnDelete(key)
			if elem, ok := s.lruIndex[key]; ok {
				s.lruOrder.Remove(elem)
				delete(s.lruIndex, key)
			}
		}

		s.backend.Store(key, newEntry)
		s.eviction.OnInsert(key, expiresAt)
		s.metrics.Puts.Add(1)

		elem := s.lruOrder.PushFront(key)
		s.lruIndex[key] = elem

		if s.events.OnPut != nil {
			s.events.OnPut(key, res.val)
		}

		if s.config.MaxEntries > 0 && s.backend.Len() > s.config.MaxEntries {
			s.evict()
		}

		return res.val, nil
	}
}

type SnapshotEntry[K comparable, V any] struct {
	Key       K
	Value     V
	ExpiresAt int64
	CreatedAt int64
	UpdatedAt int64
}

func (s *Stash[K, V]) Snapshot(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	encoder := gob.NewEncoder(w)
	now := s.clock.Now()

	var entries []SnapshotEntry[K, V]
	s.backend.Range(func(k K, v stashEntry[V]) bool {
		if v.ExpiresAt.IsZero() || now.Before(v.ExpiresAt) {
			entry := SnapshotEntry[K, V]{
				Key:       k,
				Value:     v.Value,
				CreatedAt: v.CreatedAt.UnixNano(),
				UpdatedAt: v.UpdatedAt.UnixNano(),
			}
			if !v.ExpiresAt.IsZero() {
				entry.ExpiresAt = v.ExpiresAt.UnixNano()
			}
			entries = append(entries, entry)
		}
		return true
	})

	return encoder.Encode(entries)
}

func (s *Stash[K, V]) Restore(r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	decoder := gob.NewDecoder(r)
	var entries []SnapshotEntry[K, V]
	if err := decoder.Decode(&entries); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	now := s.clock.Now()
	for _, entry := range entries {
		var expiresAt time.Time
		if entry.ExpiresAt != 0 {
			expiresAt = time.Unix(0, entry.ExpiresAt)
		}

		if !expiresAt.IsZero() && now.After(expiresAt) {
			continue
		}

		se := stashEntry[V]{
			Value:     entry.Value,
			ExpiresAt: expiresAt,
			CreatedAt: time.Unix(0, entry.CreatedAt),
			UpdatedAt: time.Unix(0, entry.UpdatedAt),
		}

		if s.wal != nil {
			keyBytes, err := s.keyCodec.Encode(entry.Key)
			if err == nil {
				valBytes, err := s.valCodec.Encode(entry.Value)
				if err == nil {
					walEntry := wal.NewSetEntryWithShard(keyBytes, valBytes, expiresAt, now, s.shardID)
					if err := s.wal.Append(walEntry); err != nil {
						s.metrics.WALErrors.Add(1)
					} else {
						s.metrics.WALWrites.Add(1)
					}
				}
			}
		}

		_, exists := s.backend.Load(entry.Key)
		s.backend.Store(entry.Key, se)

		if exists {
			s.eviction.OnUpdate(entry.Key, expiresAt)
			if elem, ok := s.lruIndex[entry.Key]; ok {
				s.lruOrder.MoveToFront(elem)
			}
		} else {
			s.eviction.OnInsert(entry.Key, expiresAt)
			elem := s.lruOrder.PushFront(entry.Key)
			s.lruIndex[entry.Key] = elem
		}
	}

	return nil
}

func (s *Stash[K, V]) CompactWAL() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	if s.wal == nil {
		return nil
	}

	if !s.ownsWAL {
		return nil
	}

	tmpPath := s.config.WALPath + ".compact"
	tmpWal, err := wal.OpenWriter(tmpPath, s.config.SyncWrites)
	if err != nil {
		return fmt.Errorf("open temp wal: %w", err)
	}

	now := s.clock.Now()
	var compactErr error

	s.backend.Range(func(k K, v stashEntry[V]) bool {
		if !v.ExpiresAt.IsZero() && now.After(v.ExpiresAt) {
			return true
		}

		keyBytes, err := s.keyCodec.Encode(k)
		if err != nil {
			return true
		}
		valBytes, err := s.valCodec.Encode(v.Value)
		if err != nil {
			return true
		}

		entry := wal.NewSetEntryWithShard(keyBytes, valBytes, v.ExpiresAt, v.UpdatedAt, s.shardID)
		if err := tmpWal.Append(entry); err != nil {
			compactErr = fmt.Errorf("write compact entry: %w", err)
			return false
		}
		return true
	})

	if compactErr != nil {
		tmpWal.Close()
		os.Remove(tmpPath)
		return compactErr
	}

	if err := tmpWal.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp wal: %w", err)
	}

	oldWalPath := s.config.WALPath + ".old"
	if err := os.Rename(s.config.WALPath, oldWalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("backup old wal: %w", err)
	}

	if err := os.Rename(tmpPath, s.config.WALPath); err != nil {
		os.Rename(oldWalPath, s.config.WALPath)
		return fmt.Errorf("rename wal: %w", err)
	}

	os.Remove(oldWalPath)

	newWal, err := wal.OpenWriter(s.config.WALPath, s.config.SyncWrites)
	if err != nil {
		return fmt.Errorf("reopen wal: %w", err)
	}
	s.wal = newWal

	return nil
}
