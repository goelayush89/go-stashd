package main

import (
	"fmt"
	"os"
	"time"

	stashd "github.com/goelayush89/go-stashd"
)

func main() {
	walPath := "./cache.wal"
	_, err := os.Stat(walPath)
	isRecovery := err == nil

	cfg := stashd.Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 0,
		WALPath:         walPath,
		SyncWrites:      true,
	}

	cache, err := stashd.Open[string, string](cfg,
		stashd.WithKeyCodec[string, string](stashd.StringCodec{}),
		stashd.WithValueCodec[string, string](stashd.StringCodec{}),
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	if isRecovery {
		fmt.Println("Recovered from WAL!")
		fmt.Println("Cache length after recovery:", cache.Len())
		if val, ok := cache.Fetch("persistent-key"); ok {
			fmt.Println("Recovered persistent-key =", val)
		}
	} else {
		fmt.Println("Fresh start - writing data...")
		cache.Put("persistent-key", "This survives crashes!")
		cache.Put("another-key", "More data")
		fmt.Println("Data written. Run again to see recovery!")
	}

	fmt.Println("\n✓ Persistence example complete!")
}
