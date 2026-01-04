package stashd

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkStash_Put(b *testing.B) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(fmt.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkStash_Fetch(b *testing.B) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		cache.Put(fmt.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Fetch(fmt.Sprintf("key-%d", i%10000))
	}
}

func BenchmarkStash_PutFetchMixed(b *testing.B) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1000)
		if i%10 == 0 {
			cache.Put(key, "value")
		} else {
			cache.Fetch(key)
		}
	}
}

func BenchmarkStash_Parallel(b *testing.B) {
	cfg := Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
	}

	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%1000)
			if i%10 == 0 {
				cache.Put(key, "value")
			} else {
				cache.Fetch(key)
			}
			i++
		}
	})
}
