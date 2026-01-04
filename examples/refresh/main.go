package main

import (
	"fmt"
	"sync/atomic"
	"time"

	stashd "github.com/goelayush89/go-stashd"
)

func main() {
	var loadCount atomic.Int32

	cfg := stashd.Config{
		DefaultTTL:      10 * time.Second,
		RefreshAfter:    5 * time.Second,
		CleanupInterval: 1 * time.Second,
	}

	loader := stashd.LoaderFunc[string, string](func(key string) (string, error) {
		count := loadCount.Add(1)
		fmt.Printf("  [Loader] Loading %s (load #%d)\n", key, count)
		time.Sleep(100 * time.Millisecond)
		return fmt.Sprintf("data-v%d", count), nil
	})

	cache, err := stashd.Open[string, string](cfg,
		stashd.WithLoader[string, string](loader),
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	fmt.Println("Refresh-Ahead Example")
	fmt.Println("=====================")
	fmt.Println("Config: TTL=10s, RefreshAfter=5s")
	fmt.Println()

	fmt.Println("1. Initial load via GetOrLoad:")
	val, _ := cache.GetOrLoad("user:123")
	fmt.Printf("   Got: %s\n\n", val)

	fmt.Println("2. Fetching within refresh window (0-5s):")
	for i := 0; i < 3; i++ {
		val, _ := cache.Fetch("user:123")
		fmt.Printf("   Fetch %d: %s (no refresh triggered)\n", i+1, val)
		time.Sleep(1 * time.Second)
	}
	fmt.Println()

	fmt.Println("3. Waiting past RefreshAfter (5s)...")
	time.Sleep(3 * time.Second)

	fmt.Println("4. Fetching after RefreshAfter (triggers background refresh):")
	val, _ = cache.Fetch("user:123")
	fmt.Printf("   Got: %s (returned immediately, refresh in background)\n", val)

	fmt.Println("5. Waiting for background refresh to complete...")
	time.Sleep(200 * time.Millisecond)

	fmt.Println("6. Fetching refreshed value:")
	val, _ = cache.Fetch("user:123")
	fmt.Printf("   Got: %s (already refreshed!)\n", val)

	fmt.Printf("\nTotal loads: %d (initial + 1 background refresh)\n", loadCount.Load())
	fmt.Println("\nRefresh-Ahead example complete!")
}
