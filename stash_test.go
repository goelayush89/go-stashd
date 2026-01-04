package stashd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goelayush89/go-stashd/clock"
)

func TestStash_PutAndFetch(t *testing.T) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0, // Disable auto cleanup for tests
	}

	cache, err := Open[string, string](cfg)
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}
	defer cache.Close()

	// Put and fetch
	if err := cache.Put("key1", "value1"); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	val, ok := cache.Fetch("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if val != "value1" {
		t.Fatalf("expected value1, got %s", val)
	}

	// Non-existent key
	_, ok = cache.Fetch("nonexistent")
	if ok {
		t.Fatal("expected nonexistent key to not exist")
	}
}

func TestStash_TTLExpiration(t *testing.T) {
	mockClock := clock.NewMock(time.Now())
	cfg := Config{
		DefaultTTL:      100 * time.Millisecond,
		CleanupInterval: 0,
	}

	cache, err := Open[string, string](cfg, WithClock[string, string](mockClock))
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}
	defer cache.Close()

	if err := cache.Put("key1", "value1"); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Should exist before expiration
	if _, ok := cache.Fetch("key1"); !ok {
		t.Fatal("expected key1 to exist before expiration")
	}

	// Advance past TTL
	mockClock.Advance(200 * time.Millisecond)

	// Should not exist after expiration
	if _, ok := cache.Fetch("key1"); ok {
		t.Fatal("expected key1 to be expired")
	}
}

func TestStash_Remove(t *testing.T) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, err := Open[string, string](cfg)
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}
	defer cache.Close()

	if err := cache.Put("key1", "value1"); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	existed, err := cache.Remove("key1")
	if err != nil {
		t.Fatalf("remove failed: %v", err)
	}
	if !existed {
		t.Fatal("expected key1 to have existed")
	}

	if cache.Has("key1") {
		t.Fatal("expected key1 to be removed")
	}

	// Remove non-existent
	existed, err = cache.Remove("nonexistent")
	if err != nil {
		t.Fatalf("remove failed: %v", err)
	}
	if existed {
		t.Fatal("expected nonexistent key to not exist")
	}
}

func TestStash_Len(t *testing.T) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, err := Open[string, string](cfg)
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}
	defer cache.Close()

	if cache.Len() != 0 {
		t.Fatalf("expected len 0, got %d", cache.Len())
	}

	cache.Put("key1", "value1")
	cache.Put("key2", "value2")

	if cache.Len() != 2 {
		t.Fatalf("expected len 2, got %d", cache.Len())
	}
}

func TestStash_Purge(t *testing.T) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, err := Open[string, string](cfg)
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "value1")
	cache.Put("key2", "value2")

	cache.Purge()

	if cache.Len() != 0 {
		t.Fatalf("expected len 0 after purge, got %d", cache.Len())
	}
}

func TestStash_DeleteExpired(t *testing.T) {
	mockClock := clock.NewMock(time.Now())
	cfg := Config{
		DefaultTTL:      100 * time.Millisecond,
		CleanupInterval: 0,
	}

	cache, err := Open[string, string](cfg, WithClock[string, string](mockClock))
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "value1")
	cache.Put("key2", "value2")

	if cache.Len() != 2 {
		t.Fatalf("expected len 2, got %d", cache.Len())
	}

	mockClock.Advance(200 * time.Millisecond)
	deleted := cache.DeleteExpired()

	if deleted != 2 {
		t.Fatalf("expected 2 deleted, got %d", deleted)
	}

	if cache.Len() != 0 {
		t.Fatalf("expected len 0, got %d", cache.Len())
	}
}

func TestStash_WALPersistence(t *testing.T) {
	// Create temp file for WAL
	tmpFile, err := os.CreateTemp("", "stash-wal-*.log")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	walPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(walPath)

	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
		WALPath:         walPath,
	}

	// Create cache and write data
	cache1, err := Open[string, string](cfg,
		WithKeyCodec[string, string](StringCodec{}),
		WithValueCodec[string, string](StringCodec{}),
	)
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}

	cache1.Put("user:1", "Alice")
	cache1.Put("user:2", "Bob")
	cache1.Remove("user:2")
	cache1.Close()

	// Reopen and verify recovery
	cache2, err := Open[string, string](cfg,
		WithKeyCodec[string, string](StringCodec{}),
		WithValueCodec[string, string](StringCodec{}),
	)
	if err != nil {
		t.Fatalf("failed to reopen stash: %v", err)
	}
	defer cache2.Close()

	// user:1 should exist
	val, ok := cache2.Fetch("user:1")
	if !ok {
		t.Fatal("expected user:1 to be recovered")
	}
	if val != "Alice" {
		t.Fatalf("expected Alice, got %s", val)
	}

	// user:2 should not exist (was deleted)
	if cache2.Has("user:2") {
		t.Fatal("expected user:2 to be deleted")
	}
}

func TestStash_WALExpiredNotRecovered(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "stash-wal-*.log")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	walPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(walPath)

	// Use mock clock to control time
	mockClock := clock.NewMock(time.Now())
	cfg := Config{
		DefaultTTL:      100 * time.Millisecond,
		CleanupInterval: 0,
		WALPath:         walPath,
	}

	// Create and write data
	cache1, err := Open[string, string](cfg,
		WithClock[string, string](mockClock),
		WithKeyCodec[string, string](StringCodec{}),
		WithValueCodec[string, string](StringCodec{}),
	)
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}

	cache1.Put("key1", "value1")
	cache1.Close()

	// Advance time past TTL
	mockClock.Advance(200 * time.Millisecond)

	// Reopen - expired entry should not be recovered
	cache2, err := Open[string, string](cfg,
		WithClock[string, string](mockClock),
		WithKeyCodec[string, string](StringCodec{}),
		WithValueCodec[string, string](StringCodec{}),
	)
	if err != nil {
		t.Fatalf("failed to reopen stash: %v", err)
	}
	defer cache2.Close()

	if cache2.Has("key1") {
		t.Fatal("expected expired key1 to not be recovered")
	}
}

func TestStash_NeverExpires(t *testing.T) {
	mockClock := clock.NewMock(time.Now())
	cfg := Config{
		DefaultTTL:      0, // Never expires
		CleanupInterval: 0,
	}

	cache, err := Open[string, string](cfg, WithClock[string, string](mockClock))
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "value1")

	// Advance time significantly
	mockClock.Advance(24 * time.Hour)

	// Should still exist
	if !cache.Has("key1") {
		t.Fatal("expected key1 to exist (never expires)")
	}
}

func TestStash_Metrics(t *testing.T) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, err := Open[string, string](cfg)
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "value1")
	cache.Put("key2", "value2")
	cache.Fetch("key1")
	cache.Fetch("key1")
	cache.Fetch("nonexistent")
	cache.Remove("key2")

	m := cache.Metrics()

	if m.Puts != 2 {
		t.Fatalf("expected 2 puts, got %d", m.Puts)
	}
	if m.Hits != 2 {
		t.Fatalf("expected 2 hits, got %d", m.Hits)
	}
	if m.Misses != 1 {
		t.Fatalf("expected 1 miss, got %d", m.Misses)
	}
	if m.Deletes != 1 {
		t.Fatalf("expected 1 delete, got %d", m.Deletes)
	}
	if m.HitRate < 0.66 || m.HitRate > 0.67 {
		t.Fatalf("expected hit rate ~0.66, got %f", m.HitRate)
	}
}

func TestStash_MetricsReset(t *testing.T) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, err := Open[string, string](cfg)
	if err != nil {
		t.Fatalf("failed to open stash: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "value1")
	cache.Fetch("key1")

	cache.ResetMetrics()
	m := cache.Metrics()

	if m.Puts != 0 || m.Hits != 0 {
		t.Fatal("expected metrics to be reset")
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"valid", Config{DefaultTTL: time.Minute}, false},
		{"negative ttl", Config{DefaultTTL: -1}, true},
		{"negative cleanup", Config{CleanupInterval: -1}, true},
		{"negative max entries", Config{MaxEntries: -1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStash_InvalidConfig(t *testing.T) {
	cfg := Config{DefaultTTL: -1}
	_, err := Open[string, string](cfg)
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
}

func TestStash_ClosedError(t *testing.T) {
	cfg := Config{DefaultTTL: time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg)
	cache.Close()

	err := cache.Put("key", "value")
	if err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}

	_, err = cache.Remove("key")
	if err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestStash_GetOrSet(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	val, existed := cache.GetOrSet("key1", "value1")
	if existed {
		t.Fatal("expected existed=false for new key")
	}
	if val != "value1" {
		t.Fatalf("expected value1, got %s", val)
	}

	val, existed = cache.GetOrSet("key1", "value2")
	if !existed {
		t.Fatal("expected existed=true for existing key")
	}
	if val != "value1" {
		t.Fatalf("expected value1, got %s", val)
	}
}

func TestStash_GetOrSetFunc(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, int](cfg)
	defer cache.Close()

	callCount := 0
	val, existed := cache.GetOrSetFunc("counter", func() int {
		callCount++
		return 42
	})

	if existed || val != 42 || callCount != 1 {
		t.Fatal("unexpected result on first call")
	}

	val, existed = cache.GetOrSetFunc("counter", func() int {
		callCount++
		return 100
	})

	if !existed || val != 42 || callCount != 1 {
		t.Fatal("func should not be called for existing key")
	}
}

func TestStash_Touch(t *testing.T) {
	mockClock := clock.NewMock(time.Now())
	cfg := Config{DefaultTTL: 100 * time.Millisecond, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg, WithClock[string, string](mockClock))
	defer cache.Close()

	cache.Put("key1", "value1")

	mockClock.Advance(80 * time.Millisecond)
	if !cache.Touch("key1") {
		t.Fatal("touch should succeed")
	}

	mockClock.Advance(80 * time.Millisecond)
	if !cache.Has("key1") {
		t.Fatal("key should still exist after touch")
	}

	mockClock.Advance(30 * time.Millisecond)
	if cache.Has("key1") {
		t.Fatal("key should be expired now")
	}

	if cache.Touch("nonexistent") {
		t.Fatal("touch should fail for nonexistent key")
	}
}

func TestStash_Keys(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	cache.Put("key1", "v1")
	cache.Put("key2", "v2")
	cache.Put("key3", "v3")

	keys := cache.Keys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
}

func TestStash_Range(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, int](cfg)
	defer cache.Close()

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	sum := 0
	cache.Range(func(k string, v int) bool {
		sum += v
		return true
	})

	if sum != 6 {
		t.Fatalf("expected sum 6, got %d", sum)
	}

	count := 0
	cache.Range(func(k string, v int) bool {
		count++
		return count < 2
	})

	if count != 2 {
		t.Fatalf("expected early exit at 2, got %d", count)
	}
}

func TestStash_LRUEviction(t *testing.T) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
		MaxEntries:      3,
	}

	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	cache.Put("a", "1")
	cache.Put("b", "2")
	cache.Put("c", "3")

	if cache.Len() != 3 {
		t.Fatalf("expected 3, got %d", cache.Len())
	}

	cache.Put("d", "4")

	if cache.Len() != 3 {
		t.Fatalf("expected 3 after eviction, got %d", cache.Len())
	}

	if cache.Has("a") {
		t.Fatal("expected 'a' to be evicted (LRU)")
	}

	if !cache.Has("d") {
		t.Fatal("expected 'd' to exist")
	}
}

func TestStash_GetOrLoad(t *testing.T) {
	loadCount := 0
	loader := LoaderFunc[string, string](func(key string) (string, error) {
		loadCount++
		return "loaded:" + key, nil
	})

	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg, WithLoader[string, string](loader))
	defer cache.Close()

	val, err := cache.GetOrLoad("key1")
	if err != nil || val != "loaded:key1" || loadCount != 1 {
		t.Fatalf("first load failed: val=%s, count=%d, err=%v", val, loadCount, err)
	}

	val, err = cache.GetOrLoad("key1")
	if err != nil || val != "loaded:key1" || loadCount != 1 {
		t.Fatal("second call should use cache, not loader")
	}
}

func TestStash_OnEvictionCallback(t *testing.T) {
	var evictedKeys []string
	var reasons []EvictionReason

	events := EventHandlers[string, string]{
		OnEviction: func(reason EvictionReason, key string, value string) {
			evictedKeys = append(evictedKeys, key)
			reasons = append(reasons, reason)
		},
	}

	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0, MaxEntries: 2}
	cache, _ := Open[string, string](cfg, WithEventHandlers[string, string](events))
	defer cache.Close()

	cache.Put("a", "1")
	cache.Put("b", "2")
	cache.Put("c", "3")

	time.Sleep(10 * time.Millisecond)

	if len(evictedKeys) != 1 || evictedKeys[0] != "a" {
		t.Fatalf("expected eviction of 'a', got %v", evictedKeys)
	}
	if reasons[0] != EvictionReasonCapacity {
		t.Fatalf("expected EvictionReasonCapacity, got %v", reasons[0])
	}
}

func TestStash_SuppressedLoader(t *testing.T) {
	loadCount := 0
	baseLoader := LoaderFunc[string, int](func(key string) (int, error) {
		loadCount++
		time.Sleep(10 * time.Millisecond)
		return 42, nil
	})

	suppressed := NewSuppressedLoader[string, int](baseLoader)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			suppressed.Load("key")
		}()
	}
	wg.Wait()

	if loadCount != 1 {
		t.Fatalf("singleflight should collapse to 1 call, got %d", loadCount)
	}
}

func TestStash_GetOrLoadWithoutLoader(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	_, err := cache.GetOrLoad("key")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestStash_OnPutCallback(t *testing.T) {
	var putCount atomic.Int32

	events := EventHandlers[string, string]{
		OnPut: func(key, value string) {
			putCount.Add(1)
		},
	}

	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg, WithEventHandlers[string, string](events))
	defer cache.Close()

	cache.Put("a", "1")
	cache.Put("b", "2")

	time.Sleep(50 * time.Millisecond)

	if putCount.Load() != 2 {
		t.Fatalf("expected 2 OnPut calls, got %d", putCount.Load())
	}
}

func TestStash_LRUAccessUpdatesOrder(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0, MaxEntries: 3}
	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	cache.Put("a", "1")
	cache.Put("b", "2")
	cache.Put("c", "3")

	cache.Fetch("a")

	cache.Put("d", "4")

	if !cache.Has("a") {
		t.Fatal("'a' should still exist (recently accessed)")
	}
	if cache.Has("b") {
		t.Fatal("'b' should be evicted (LRU)")
	}
}

func TestStash_ConcurrentAccess(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, int](cfg)
	defer cache.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			cache.Put(fmt.Sprintf("key%d", i), i)
		}(i)
		go func(i int) {
			defer wg.Done()
			cache.Fetch(fmt.Sprintf("key%d", i))
		}(i)
		go func(i int) {
			defer wg.Done()
			cache.Has(fmt.Sprintf("key%d", i))
		}(i)
	}
	wg.Wait()
}

func TestStash_CleanupLoop(t *testing.T) {
	mockClock := clock.NewMock(time.Now())
	cfg := Config{
		DefaultTTL:      50 * time.Millisecond,
		CleanupInterval: 10 * time.Millisecond,
	}

	cache, _ := Open[string, string](cfg, WithClock[string, string](mockClock))

	cache.Put("key1", "value1")

	mockClock.Advance(100 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	cache.Close()
}

func TestStash_EvictionReasonString(t *testing.T) {
	if EvictionReasonDeleted.String() != "deleted" {
		t.Fatal("wrong string for deleted")
	}
	if EvictionReasonExpired.String() != "expired" {
		t.Fatal("wrong string for expired")
	}
	if EvictionReasonCapacity.String() != "capacity" {
		t.Fatal("wrong string for capacity")
	}
	if EvictionReason(99).String() != "unknown" {
		t.Fatal("wrong string for unknown")
	}
}

func TestStash_JSONCodec(t *testing.T) {
	type Data struct {
		Value int `json:"value"`
	}

	tmpFile, _ := os.CreateTemp("", "stash-*.wal")
	walPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(walPath)

	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0, WALPath: walPath}
	cache, _ := Open[string, Data](cfg,
		WithKeyCodec[string, Data](StringCodec{}),
		WithValueCodec[string, Data](JSONCodec[Data]{}),
	)

	cache.Put("test", Data{Value: 42})
	cache.Close()

	cache2, _ := Open[string, Data](cfg,
		WithKeyCodec[string, Data](StringCodec{}),
		WithValueCodec[string, Data](JSONCodec[Data]{}),
	)
	defer cache2.Close()

	val, ok := cache2.Fetch("test")
	if !ok || val.Value != 42 {
		t.Fatalf("expected {Value:42}, got %+v", val)
	}
}

func TestStash_DefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.DefaultTTL != 5*time.Minute {
		t.Fatal("wrong default TTL")
	}
	if cfg.CleanupInterval != 1*time.Minute {
		t.Fatal("wrong default cleanup interval")
	}
}

func TestStash_GetAndDelete(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	cache.Put("key", "value")

	val, found, err := cache.GetAndDelete("key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found || val != "value" {
		t.Fatalf("expected value, got %v, found=%v", val, found)
	}

	_, found2, _ := cache.GetAndDelete("key")
	if found2 {
		t.Fatal("key should not exist after GetAndDelete")
	}

	if cache.Len() != 0 {
		t.Fatalf("expected empty cache, got %d items", cache.Len())
	}
}

func TestStash_SetIfAbsent(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	inserted, err := cache.SetIfAbsent("key", "v1")
	if err != nil || !inserted {
		t.Fatalf("expected insert, got inserted=%v, err=%v", inserted, err)
	}

	inserted2, err := cache.SetIfAbsent("key", "v2")
	if err != nil || inserted2 {
		t.Fatal("should not insert on existing key")
	}

	val, _ := cache.Fetch("key")
	if val != "v1" {
		t.Fatalf("expected v1, got %v", val)
	}
}

func TestStash_SetIfPresent(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	updated, err := cache.SetIfPresent("key", "v1")
	if err != nil || updated {
		t.Fatal("should not update non-existing key")
	}

	cache.Put("key", "original")

	updated2, err := cache.SetIfPresent("key", "updated")
	if err != nil || !updated2 {
		t.Fatalf("expected update, got updated=%v, err=%v", updated2, err)
	}

	val, _ := cache.Fetch("key")
	if val != "updated" {
		t.Fatalf("expected updated, got %v", val)
	}
}

func TestStash_DeleteFunc(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, int](cfg)
	defer cache.Close()

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)
	cache.Put("d", 4)

	deleted := cache.DeleteFunc(func(key string, value int) bool {
		return value%2 == 0
	})

	if deleted != 2 {
		t.Fatalf("expected 2 deletions, got %d", deleted)
	}

	if cache.Len() != 2 {
		t.Fatalf("expected 2 remaining, got %d", cache.Len())
	}

	if cache.Has("b") || cache.Has("d") {
		t.Fatal("even values should be deleted")
	}
}

func TestStash_Size(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	if cache.Size() != 0 {
		t.Fatalf("empty cache should have size 0, got %d", cache.Size())
	}

	cache.Put("a", "1")
	cache.Put("b", "2")

	size := cache.Size()
	if size <= 0 {
		t.Fatalf("cache with items should have positive size, got %d", size)
	}
}

func TestStash_FetchWithTTL(t *testing.T) {
	mockClock := clock.NewMock(time.Now())
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg, WithClock[string, string](mockClock))
	defer cache.Close()

	cache.Put("key", "value")

	item, found := cache.FetchWithTTL("key")
	if !found {
		t.Fatal("expected to find key")
	}
	if item.Value != "value" {
		t.Fatalf("expected 'value', got %v", item.Value)
	}
	if item.TTL <= 0 || item.TTL > 5*time.Minute {
		t.Fatalf("unexpected TTL: %v", item.TTL)
	}

	mockClock.Advance(2 * time.Minute)
	item2, _ := cache.FetchWithTTL("key")
	if item2.TTL >= item.TTL {
		t.Fatal("TTL should decrease over time")
	}

	_, found2 := cache.FetchWithTTL("missing")
	if found2 {
		t.Fatal("should not find missing key")
	}
}

func TestStash_GetOrLoadWithContext(t *testing.T) {
	callCount := 0
	loader := LoaderFunc[string, string](func(key string) (string, error) {
		callCount++
		time.Sleep(10 * time.Millisecond)
		return "loaded:" + key, nil
	})

	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg, WithLoader[string, string](loader))
	defer cache.Close()

	ctx := context.Background()
	val, err := cache.GetOrLoadWithContext(ctx, "key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "loaded:key" {
		t.Fatalf("expected 'loaded:key', got %v", val)
	}

	val2, _ := cache.GetOrLoadWithContext(ctx, "key")
	if val2 != "loaded:key" {
		t.Fatalf("expected cached value, got %v", val2)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 loader call, got %d", callCount)
	}
}

func TestStash_GetOrLoadWithContext_Timeout(t *testing.T) {
	loader := LoaderFunc[string, string](func(key string) (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "loaded", nil
	})

	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache, _ := Open[string, string](cfg, WithLoader[string, string](loader))
	defer cache.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := cache.GetOrLoadWithContext(ctx, "key")
	if err == nil || err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestStash_SnapshotRestore(t *testing.T) {
	cfg := Config{DefaultTTL: 5 * time.Minute, CleanupInterval: 0}
	cache1, _ := Open[string, string](cfg)
	defer cache1.Close()

	cache1.Put("a", "1")
	cache1.Put("b", "2")
	cache1.Put("c", "3")

	var buf bytes.Buffer
	if err := cache1.Snapshot(&buf); err != nil {
		t.Fatalf("snapshot error: %v", err)
	}

	cache2, _ := Open[string, string](cfg)
	defer cache2.Close()

	if err := cache2.Restore(&buf); err != nil {
		t.Fatalf("restore error: %v", err)
	}

	if cache2.Len() != 3 {
		t.Fatalf("expected 3 items after restore, got %d", cache2.Len())
	}

	val, found := cache2.Fetch("b")
	if !found || val != "2" {
		t.Fatalf("expected '2', got %v", val)
	}
}
