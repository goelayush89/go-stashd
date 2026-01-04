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
	}

	cache, err := stashd.Open[string, string](cfg)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	cache.Put("user:1", "Alice")
	cache.Put("user:2", "Bob")
	cache.PutWithTTL("session:abc", "data", 30*time.Second)

	if val, ok := cache.Fetch("user:1"); ok {
		fmt.Println("user:1 =", val)
	}

	fmt.Println("Has user:2?", cache.Has("user:2"))
	fmt.Println("Has user:999?", cache.Has("user:999"))

	cache.Remove("user:2")
	fmt.Println("After remove, has user:2?", cache.Has("user:2"))

	fmt.Println("Cache length:", cache.Len())
	fmt.Println("\n✓ Basic example complete!")
}
