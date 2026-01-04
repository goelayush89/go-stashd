package stashd

import (
	"context"
	"encoding/gob"
	"fmt"
	"hash/maphash"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/goelayush89/go-stashd/clock"
	"github.com/goelayush89/go-stashd/storage/wal"
)

type ShardedStash[K comparable, V any] struct {
	shards    []*Stash[K, V]
	shardMask uint64
	seed      maphash.Seed
	config    ShardedConfig
	wal       *wal.Writer
	keyCodec  KeyCodec[K]
	valCodec  ValueCodec[V]
	closed    bool
	mu        sync.RWMutex
}

type ShardedConfig struct {
	Shards          int
	DefaultTTL      time.Duration
	CleanupInterval time.Duration
	MaxEntriesTotal int
	WALPath         string
	SyncWrites      bool
	EvictionPolicy  EvictionPolicy
	RefreshAfter    time.Duration
}

type ShardedOption[K comparable, V any] func(*shardedOptions[K, V])

type shardedOptions[K comparable, V any] struct {
	clock    clock.Clock
	keyCodec KeyCodec[K]
	valCodec ValueCodec[V]
	loader   Loader[K, V]
	events   EventHandlers[K, V]
}

func WithShardedClock[K comparable, V any](c clock.Clock) ShardedOption[K, V] {
	return func(o *shardedOptions[K, V]) { o.clock = c }
}

func WithShardedKeyCodec[K comparable, V any](codec KeyCodec[K]) ShardedOption[K, V] {
	return func(o *shardedOptions[K, V]) { o.keyCodec = codec }
}

func WithShardedValueCodec[K comparable, V any](codec ValueCodec[V]) ShardedOption[K, V] {
	return func(o *shardedOptions[K, V]) { o.valCodec = codec }
}

func WithShardedLoader[K comparable, V any](loader Loader[K, V]) ShardedOption[K, V] {
	return func(o *shardedOptions[K, V]) { o.loader = loader }
}

func WithShardedEventHandlers[K comparable, V any](handlers EventHandlers[K, V]) ShardedOption[K, V] {
	return func(o *shardedOptions[K, V]) { o.events = handlers }
}

func OpenSharded[K comparable, V any](cfg ShardedConfig, opts ...ShardedOption[K, V]) (*ShardedStash[K, V], error) {
	numShards := cfg.Shards
	if numShards <= 0 {
		numShards = 16
	}

	numShards = nextPowerOfTwo(numShards)

	options := &shardedOptions[K, V]{}
	for _, opt := range opts {
		opt(options)
	}

	if cfg.WALPath != "" && (options.keyCodec == nil || options.valCodec == nil) {
		return nil, ErrCodecRequired
	}

	maxPerShard := 0
	if cfg.MaxEntriesTotal > 0 {
		maxPerShard = cfg.MaxEntriesTotal / numShards
		if maxPerShard < 1 {
			maxPerShard = 1
		}
	}

	var unifiedWAL *wal.Writer

	if cfg.WALPath != "" {
		dir := filepath.Dir(cfg.WALPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create wal directory: %w", err)
		}
	}

	shards := make([]*Stash[K, V], numShards)
	for i := 0; i < numShards; i++ {
		shardCfg := Config{
			DefaultTTL:      cfg.DefaultTTL,
			CleanupInterval: cfg.CleanupInterval,
			MaxEntries:      maxPerShard,
			EvictionPolicy:  cfg.EvictionPolicy,
			RefreshAfter:    cfg.RefreshAfter,
		}

		var shardOpts []Option[K, V]
		if options.clock != nil {
			shardOpts = append(shardOpts, WithClock[K, V](options.clock))
		}
		if options.keyCodec != nil {
			shardOpts = append(shardOpts, WithKeyCodec[K, V](options.keyCodec))
		}
		if options.valCodec != nil {
			shardOpts = append(shardOpts, WithValueCodec[K, V](options.valCodec))
		}
		if options.loader != nil {
			shardOpts = append(shardOpts, WithLoader[K, V](options.loader))
		}
		if options.events.OnPut != nil || options.events.OnUpdate != nil ||
			options.events.OnEviction != nil || options.events.OnHit != nil {
			shardOpts = append(shardOpts, WithEventHandlers[K, V](options.events))
		}

		shard, err := Open[K, V](shardCfg, shardOpts...)
		if err != nil {
			for j := 0; j < i; j++ {
				shards[j].Close()
			}
			return nil, fmt.Errorf("create shard %d: %w", i, err)
		}
		shards[i] = shard
	}

	ss := &ShardedStash[K, V]{
		shards:    shards,
		shardMask: uint64(numShards - 1),
		seed:      maphash.MakeSeed(),
		config:    cfg,
		keyCodec:  options.keyCodec,
		valCodec:  options.valCodec,
	}

	if cfg.WALPath != "" {
		if err := ss.recoverFromWAL(); err != nil {
			for _, shard := range shards {
				shard.Close()
			}
			return nil, fmt.Errorf("wal recovery: %w", err)
		}

		w, err := wal.OpenWriter(cfg.WALPath, cfg.SyncWrites)
		if err != nil {
			for _, shard := range shards {
				shard.Close()
			}
			return nil, fmt.Errorf("open wal writer: %w", err)
		}
		unifiedWAL = w
		ss.wal = unifiedWAL

		for i, shard := range shards {
			shard.wal = unifiedWAL
			shard.shardID = uint16(i)
			shard.ownsWAL = false
		}
	}

	return ss, nil
}

func (s *ShardedStash[K, V]) getShard(key K) *Stash[K, V] {
	h := s.hashKey(key)
	idx := h & s.shardMask
	return s.shards[idx]
}

func (s *ShardedStash[K, V]) hashKey(key K) uint64 {
	var h maphash.Hash
	h.SetSeed(s.seed)
	switch k := any(key).(type) {
	case string:
		h.WriteString(k)
	case []byte:
		h.Write(k)
	case int:
		h.WriteString(fmt.Sprint(k))
	case int64:
		h.WriteString(fmt.Sprint(k))
	case uint64:
		h.WriteString(fmt.Sprint(k))
	default:
		h.WriteString(fmt.Sprintf("%v", key))
	}
	return h.Sum64()
}

func (s *ShardedStash[K, V]) Put(key K, value V) error {
	return s.getShard(key).Put(key, value)
}

func (s *ShardedStash[K, V]) PutWithTTL(key K, value V, ttl time.Duration) error {
	return s.getShard(key).PutWithTTL(key, value, ttl)
}

func (s *ShardedStash[K, V]) Fetch(key K) (V, bool) {
	return s.getShard(key).Fetch(key)
}

func (s *ShardedStash[K, V]) FetchWithTTL(key K) (Item[V], bool) {
	return s.getShard(key).FetchWithTTL(key)
}

func (s *ShardedStash[K, V]) Has(key K) bool {
	return s.getShard(key).Has(key)
}

func (s *ShardedStash[K, V]) Remove(key K) (bool, error) {
	return s.getShard(key).Remove(key)
}

func (s *ShardedStash[K, V]) GetAndDelete(key K) (V, bool, error) {
	return s.getShard(key).GetAndDelete(key)
}

func (s *ShardedStash[K, V]) SetIfAbsent(key K, value V) (bool, error) {
	return s.getShard(key).SetIfAbsent(key, value)
}

func (s *ShardedStash[K, V]) SetIfPresent(key K, value V) (bool, error) {
	return s.getShard(key).SetIfPresent(key, value)
}

func (s *ShardedStash[K, V]) GetOrSet(key K, value V) (V, bool) {
	return s.getShard(key).GetOrSet(key, value)
}

func (s *ShardedStash[K, V]) GetOrSetFunc(key K, fn func() V) (V, bool) {
	return s.getShard(key).GetOrSetFunc(key, fn)
}

func (s *ShardedStash[K, V]) GetOrLoad(key K) (V, error) {
	return s.getShard(key).GetOrLoad(key)
}

func (s *ShardedStash[K, V]) GetOrLoadWithContext(ctx context.Context, key K) (V, error) {
	return s.getShard(key).GetOrLoadWithContext(ctx, key)
}

func (s *ShardedStash[K, V]) Touch(key K) bool {
	return s.getShard(key).Touch(key)
}

func (s *ShardedStash[K, V]) Len() int {
	total := 0
	for _, shard := range s.shards {
		total += shard.Len()
	}
	return total
}

func (s *ShardedStash[K, V]) Size() int64 {
	var total int64
	for _, shard := range s.shards {
		total += shard.Size()
	}
	return total
}

func (s *ShardedStash[K, V]) Keys() []K {
	var allKeys []K
	for _, shard := range s.shards {
		allKeys = append(allKeys, shard.Keys()...)
	}
	return allKeys
}

func (s *ShardedStash[K, V]) Range(fn func(key K, value V) bool) {
	for _, shard := range s.shards {
		cont := true
		shard.Range(func(k K, v V) bool {
			if !fn(k, v) {
				cont = false
				return false
			}
			return true
		})
		if !cont {
			break
		}
	}
}

func (s *ShardedStash[K, V]) DeleteFunc(fn func(key K, value V) bool) int {
	total := 0
	for _, shard := range s.shards {
		total += shard.DeleteFunc(fn)
	}
	return total
}

func (s *ShardedStash[K, V]) DeleteExpired() int {
	total := 0
	for _, shard := range s.shards {
		total += shard.DeleteExpired()
	}
	return total
}

func (s *ShardedStash[K, V]) Purge() error {
	for i, shard := range s.shards {
		if err := shard.Purge(); err != nil {
			return fmt.Errorf("purge shard %d: %w", i, err)
		}
	}
	return nil
}

func (s *ShardedStash[K, V]) Metrics() MetricsSnapshot {
	var combined MetricsSnapshot
	for _, shard := range s.shards {
		m := shard.Metrics()
		combined.Hits += m.Hits
		combined.Misses += m.Misses
		combined.Puts += m.Puts
		combined.Deletes += m.Deletes
		combined.Evictions += m.Evictions
		combined.WALWrites += m.WALWrites
		combined.WALErrors += m.WALErrors
	}
	total := combined.Hits + combined.Misses
	if total > 0 {
		combined.HitRate = float64(combined.Hits) / float64(total)
	}
	return combined
}

func (s *ShardedStash[K, V]) ResetMetrics() {
	for _, shard := range s.shards {
		shard.ResetMetrics()
	}
}

func (s *ShardedStash[K, V]) Snapshot(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return ErrClosed
	}

	enc := gob.NewEncoder(w)
	for i, shard := range s.shards {
		keys := shard.Keys()
		for _, key := range keys {
			val, ok := shard.Fetch(key)
			if !ok {
				continue
			}
			entry := snapshotEntry[K, V]{ShardID: uint16(i), Key: key, Value: val}
			if err := enc.Encode(entry); err != nil {
				return fmt.Errorf("encode entry: %w", err)
			}
		}
	}
	return nil
}

type snapshotEntry[K comparable, V any] struct {
	ShardID uint16
	Key     K
	Value   V
}

func (s *ShardedStash[K, V]) Restore(r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	dec := gob.NewDecoder(r)
	for {
		var entry snapshotEntry[K, V]
		if err := dec.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decode entry: %w", err)
		}
		if int(entry.ShardID) < len(s.shards) {
			s.shards[entry.ShardID].Put(entry.Key, entry.Value)
		}
	}
	return nil
}

func (s *ShardedStash[K, V]) CompactWAL() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	if s.wal == nil {
		return nil
	}

	if s.keyCodec == nil || s.valCodec == nil {
		return nil
	}

	tmpPath := s.config.WALPath + ".compact"
	tmpWal, err := wal.OpenWriter(tmpPath, s.config.SyncWrites)
	if err != nil {
		return fmt.Errorf("open temp wal: %w", err)
	}

	now := time.Now()
	var compactErr error

	for shardIdx, shard := range s.shards {
		shard.mu.RLock()
		shard.backend.Range(func(k K, v stashEntry[V]) bool {
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

			entry := wal.NewSetEntryWithShard(keyBytes, valBytes, v.ExpiresAt, v.UpdatedAt, uint16(shardIdx))
			if err := tmpWal.Append(entry); err != nil {
				compactErr = fmt.Errorf("write compact entry: %w", err)
				return false
			}
			return true
		})
		shard.mu.RUnlock()
		if compactErr != nil {
			break
		}
	}

	if compactErr != nil {
		tmpWal.Close()
		os.Remove(tmpPath)
		return compactErr
	}

	if err := tmpWal.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp wal: %w", err)
	}

	if err := s.wal.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close old wal: %w", err)
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

	for _, shard := range s.shards {
		shard.wal = newWal
	}

	return nil
}

func (s *ShardedStash[K, V]) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	var firstErr error
	for i, shard := range s.shards {
		if err := shard.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close shard %d: %w", i, err)
		}
	}

	if s.wal != nil {
		if err := s.wal.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close wal: %w", err)
		}
	}

	return firstErr
}

func (s *ShardedStash[K, V]) recoverFromWAL() error {
	if s.keyCodec == nil || s.valCodec == nil {
		return nil
	}

	return wal.Recover(s.config.WALPath, func(entry *wal.Entry) bool {
		shardID := int(entry.Header.ShardID)
		if shardID >= len(s.shards) {
			return true
		}

		shard := s.shards[shardID]

		key, err := s.keyCodec.Decode(entry.Key)
		if err != nil {
			return true
		}

		switch entry.Header.OpType {
		case wal.OpSet:
			value, err := s.valCodec.Decode(entry.Value)
			if err != nil {
				return true
			}

			var expiresAt time.Time
			if entry.Header.ExpiresAt != 0 {
				expiresAt = time.Unix(0, entry.Header.ExpiresAt)
			}

			if !expiresAt.IsZero() && time.Now().After(expiresAt) {
				return true
			}

			shard.mu.Lock()
			se := stashEntry[V]{
				Value:     value,
				ExpiresAt: expiresAt,
				CreatedAt: time.Unix(0, entry.Header.Timestamp),
				UpdatedAt: time.Unix(0, entry.Header.Timestamp),
			}
			_, exists := shard.backend.Load(key)
			shard.backend.Store(key, se)
			if exists {
				shard.eviction.OnUpdate(key, expiresAt)
			} else {
				shard.eviction.OnInsert(key, expiresAt)
				elem := shard.lruOrder.PushFront(key)
				shard.lruIndex[key] = elem
			}
			shard.mu.Unlock()

		case wal.OpDelete:
			shard.mu.Lock()
			shard.backend.Delete(key)
			shard.eviction.OnDelete(key)
			if elem, ok := shard.lruIndex[key]; ok {
				shard.lruOrder.Remove(elem)
				delete(shard.lruIndex, key)
			}
			shard.mu.Unlock()
		}

		return true
	})
}

func (s *ShardedStash[K, V]) ShardCount() int {
	return len(s.shards)
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
