package main

import (
	"fmt"
	"time"

	stashd "github.com/goelayush89/go-stashd"
)

func main() {
	cfg := stashd.Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 1 * time.Minute,
		MaxEntries:      1000,
		EvictionPolicy:  stashd.EvictionPolicyTinyLFU,
	}

	cache, err := stashd.Open[string, string](cfg)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	fmt.Println("TinyLFU Eviction Example")
	fmt.Println("===========================")

	for i := 0; i < 500; i++ {
		cache.Put(fmt.Sprintf("item:%d", i), fmt.Sprintf("value-%d", i))
	}
	fmt.Println("Added 500 items to cache")

	fmt.Println("\nSimulating hot keys (frequently accessed)...")
	for i := 0; i < 100; i++ {
		cache.Fetch("item:0")
		cache.Fetch("item:1")
		cache.Fetch("item:2")
	}
	fmt.Println("Keys item:0, item:1, item:2 accessed 100 times each")

	fmt.Println("\nAdding 600 more items (triggers eviction)...")
	for i := 500; i < 1100; i++ {
		cache.Put(fmt.Sprintf("item:%d", i), fmt.Sprintf("value-%d", i))
	}

	fmt.Println("\nChecking hot keys survived eviction:")
	hotKeys := []string{"item:0", "item:1", "item:2"}
	for _, key := range hotKeys {
		if val, ok := cache.Fetch(key); ok {
			fmt.Printf("  ✓ %s = %s (survived due to high frequency)\n", key, val)
		} else {
			fmt.Printf("  ✗ %s was evicted\n", key)
		}
	}

	coldKey := "item:100"
	if _, ok := cache.Fetch(coldKey); ok {
		fmt.Printf("\n  %s still exists\n", coldKey)
	} else {
		fmt.Printf("\n  ✗ %s was evicted (low frequency)\n", coldKey)
	}

	m := cache.Metrics()
	fmt.Printf("\nCache Statistics:\n")
	fmt.Printf("  Entries: %d\n", cache.Len())
	fmt.Printf("  Hits: %d, Misses: %d\n", m.Hits, m.Misses)
	fmt.Printf("  Hit Rate: %.1f%%\n", m.HitRate*100)
	fmt.Printf("  Evictions: %d\n", m.Evictions)

	fmt.Println("\n✓ TinyLFU example complete!")
}
