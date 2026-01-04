package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	stashd "github.com/goelayush89/go-stashd"
)

func main() {
	cfg := stashd.ShardedConfig{
		Shards:          32,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 1 * time.Minute,
		MaxEntriesTotal: 10000,
	}

	cache, err := stashd.OpenSharded[string, int](cfg)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	fmt.Println("Sharded Cache Example")
	fmt.Println("========================")
	fmt.Printf("Running with %d shards\n\n", cache.ShardCount())

	fmt.Println("Testing basic operations...")
	cache.Put("user:1", 100)
	cache.Put("user:2", 200)
	cache.Put("user:3", 300)

	if val, ok := cache.Fetch("user:1"); ok {
		fmt.Printf("  user:1 = %d\n", val)
	}

	val, existed := cache.GetOrSet("user:4", 400)
	fmt.Printf("  GetOrSet user:4 = %d (existed: %v)\n", val, existed)

	removed, _ := cache.Remove("user:3")
	fmt.Printf("  Removed user:3: %v\n", removed)

	fmt.Printf("\nCache length: %d\n", cache.Len())

	fmt.Println("\n--- Concurrent Performance Test ---")

	numGoroutines := 100
	opsPerGoroutine := 1000

	var wg sync.WaitGroup
	var totalOps atomic.Int64

	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("key:%d:%d", id, i%100)
				if i%3 == 0 {
					cache.Put(key, i)
				} else {
					cache.Fetch(key)
				}
				totalOps.Add(1)
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Completed %d operations in %v\n", totalOps.Load(), elapsed)
	fmt.Printf("Throughput: %.0f ops/sec\n", float64(totalOps.Load())/elapsed.Seconds())

	m := cache.Metrics()
	fmt.Printf("\nAggregate Metrics (across all shards):\n")
	fmt.Printf("  Puts: %d\n", m.Puts)
	fmt.Printf("  Hits: %d, Misses: %d\n", m.Hits, m.Misses)
	fmt.Printf("  Hit Rate: %.1f%%\n", m.HitRate*100)
	fmt.Printf("  Total Entries: %d\n", cache.Len())

	fmt.Println("\n--- Testing Other Methods ---")

	keys := cache.Keys()
	fmt.Printf("Total keys: %d\n", len(keys))

	count := 0
	cache.Range(func(k string, v int) bool {
		count++
		return count < 5
	})
	fmt.Printf("Iterated first %d items via Range\n", count)

	deleted := cache.DeleteFunc(func(k string, v int) bool {
		return v > 500
	})
	fmt.Printf("DeleteFunc removed %d items with value > 500\n", deleted)

	cache.ResetMetrics()
	fmt.Println("Metrics reset")

	fmt.Println("\n✓ Sharded cache example complete!")
}
