package stashd

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/goelayush89/go-stashd/clock"
)

func TestTinyLFU_BasicOperations(t *testing.T) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
		MaxEntries:      100,
		EvictionPolicy:  EvictionPolicyTinyLFU,
	}

	cache, err := Open[string, string](cfg)
	if err != nil {
		t.Fatalf("failed to open cache: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "value1")
	cache.Put("key2", "value2")

	val, ok := cache.Fetch("key1")
	if !ok || val != "value1" {
		t.Fatalf("expected value1, got %v", val)
	}

	if cache.Len() != 2 {
		t.Fatalf("expected len 2, got %d", cache.Len())
	}
}

func TestTinyLFU_FrequencyBasedEviction(t *testing.T) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
		MaxEntries:      5,
		EvictionPolicy:  EvictionPolicyTinyLFU,
	}

	cache, _ := Open[string, int](cfg)
	defer cache.Close()

	for i := 0; i < 5; i++ {
		cache.Put(fmt.Sprintf("key%d", i), i)
	}

	for i := 0; i < 10; i++ {
		cache.Fetch("key0")
		cache.Fetch("key1")
	}

	cache.Put("newkey", 100)

	if !cache.Has("key0") {
		t.Fatal("frequently accessed key0 should still exist")
	}
	if !cache.Has("key1") {
		t.Fatal("frequently accessed key1 should still exist")
	}
}

func TestTinyLFU_TTLExpiration(t *testing.T) {
	mockClock := clock.NewMock(time.Now())
	cfg := Config{
		DefaultTTL:      100 * time.Millisecond,
		CleanupInterval: 0,
		MaxEntries:      100,
		EvictionPolicy:  EvictionPolicyTinyLFU,
	}

	cache, _ := Open[string, string](cfg, WithClock[string, string](mockClock))
	defer cache.Close()

	cache.Put("key1", "value1")

	if !cache.Has("key1") {
		t.Fatal("key should exist before expiration")
	}

	mockClock.Advance(200 * time.Millisecond)

	if cache.Has("key1") {
		t.Fatal("key should be expired")
	}
}

func TestShardedStash_BasicOperations(t *testing.T) {
	cfg := ShardedConfig{
		Shards:          16,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
		MaxEntriesTotal: 1000,
	}

	cache, err := OpenSharded[string, string](cfg)
	if err != nil {
		t.Fatalf("failed to open sharded cache: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "value1")
	cache.Put("key2", "value2")

	val, ok := cache.Fetch("key1")
	if !ok || val != "value1" {
		t.Fatalf("expected value1, got %v", val)
	}

	if cache.Len() != 2 {
		t.Fatalf("expected len 2, got %d", cache.Len())
	}

	if cache.ShardCount() != 16 {
		t.Fatalf("expected 16 shards, got %d", cache.ShardCount())
	}
}

func TestShardedStash_Distribution(t *testing.T) {
	cfg := ShardedConfig{
		Shards:          8,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, _ := OpenSharded[string, int](cfg)
	defer cache.Close()

	for i := 0; i < 1000; i++ {
		cache.Put(fmt.Sprintf("key%d", i), i)
	}

	if cache.Len() != 1000 {
		t.Fatalf("expected 1000 items, got %d", cache.Len())
	}

	m := cache.Metrics()
	if m.Puts != 1000 {
		t.Fatalf("expected 1000 puts, got %d", m.Puts)
	}
}

func TestShardedStash_Concurrent(t *testing.T) {
	cfg := ShardedConfig{
		Shards:          32,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, _ := OpenSharded[string, int](cfg)
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

func TestShardedStash_AllMethods(t *testing.T) {
	cfg := ShardedConfig{
		Shards:          4,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, _ := OpenSharded[string, string](cfg)
	defer cache.Close()

	cache.Put("a", "1")
	cache.PutWithTTL("b", "2", time.Minute)

	val, existed := cache.GetOrSet("c", "3")
	if existed || val != "3" {
		t.Fatal("GetOrSet failed")
	}

	inserted, _ := cache.SetIfAbsent("d", "4")
	if !inserted {
		t.Fatal("SetIfAbsent should insert")
	}

	cache.Put("e", "5")
	updated, _ := cache.SetIfPresent("e", "55")
	if !updated {
		t.Fatal("SetIfPresent should update")
	}

	val, found, _ := cache.GetAndDelete("e")
	if !found || val != "55" {
		t.Fatal("GetAndDelete failed")
	}

	cache.Touch("a")

	keys := cache.Keys()
	if len(keys) != 4 {
		t.Fatalf("expected 4 keys, got %d", len(keys))
	}

	count := 0
	cache.Range(func(k, v string) bool {
		count++
		return true
	})
	if count != 4 {
		t.Fatalf("expected 4 in range, got %d", count)
	}

	deleted := cache.DeleteFunc(func(k, v string) bool {
		return k == "a"
	})
	if deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}

	cache.ResetMetrics()
	m := cache.Metrics()
	if m.Hits != 0 {
		t.Fatal("metrics should be reset")
	}
}

func TestShardedStash_Eviction(t *testing.T) {
	cfg := ShardedConfig{
		Shards:          4,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
		MaxEntriesTotal: 20,
	}

	cache, _ := OpenSharded[string, int](cfg)
	defer cache.Close()

	for i := 0; i < 100; i++ {
		cache.Put(fmt.Sprintf("key%d", i), i)
	}

	if cache.Len() > 20 {
		t.Fatalf("expected max 20 entries, got %d", cache.Len())
	}
}

func BenchmarkShardedStash_Put(b *testing.B) {
	cfg := ShardedConfig{
		Shards:          64,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, _ := OpenSharded[string, string](cfg)
	defer cache.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Put(fmt.Sprintf("key-%d", i), "value")
			i++
		}
	})
}

func BenchmarkShardedStash_Fetch(b *testing.B) {
	cfg := ShardedConfig{
		Shards:          64,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, _ := OpenSharded[string, string](cfg)
	defer cache.Close()

	for i := 0; i < 10000; i++ {
		cache.Put(fmt.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Fetch(fmt.Sprintf("key-%d", i%10000))
			i++
		}
	})
}

func BenchmarkTinyLFU_Put(b *testing.B) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
		MaxEntries:      100000,
		EvictionPolicy:  EvictionPolicyTinyLFU,
	}

	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(fmt.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkTinyLFU_Fetch(b *testing.B) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
		MaxEntries:      100000,
		EvictionPolicy:  EvictionPolicyTinyLFU,
	}

	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	for i := 0; i < 10000; i++ {
		cache.Put(fmt.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Fetch(fmt.Sprintf("key-%d", i%10000))
	}
}
