package stashd

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/goelayush89/go-stashd/clock"
)

func TestRefreshAhead_Basic(t *testing.T) {
	mockClock := clock.NewMock(time.Now())
	var loadCount atomic.Int32

	cfg := Config{
		DefaultTTL:      100 * time.Millisecond,
		RefreshAfter:    50 * time.Millisecond,
		CleanupInterval: 0,
	}

	loader := LoaderFunc[string, string](func(key string) (string, error) {
		loadCount.Add(1)
		return "refreshed-" + key, nil
	})

	cache, err := Open[string, string](cfg,
		WithClock[string, string](mockClock),
		WithLoader[string, string](loader),
	)
	if err != nil {
		t.Fatalf("failed to open cache: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "initial")

	val, ok := cache.Fetch("key1")
	if !ok || val != "initial" {
		t.Fatalf("expected initial, got %v", val)
	}

	mockClock.Advance(60 * time.Millisecond)

	val, ok = cache.Fetch("key1")
	if !ok || val != "initial" {
		t.Fatalf("expected initial (stale), got %v", val)
	}

	time.Sleep(50 * time.Millisecond)

	cache.mu.Lock()
	cache.mu.Unlock()

	val, ok = cache.Fetch("key1")
	if !ok {
		t.Fatal("key should still exist")
	}

	if loadCount.Load() != 1 {
		t.Fatalf("expected 1 refresh, got %d", loadCount.Load())
	}
}

func TestRefreshAhead_NoLoaderNoRefresh(t *testing.T) {
	mockClock := clock.NewMock(time.Now())

	cfg := Config{
		DefaultTTL:      100 * time.Millisecond,
		RefreshAfter:    50 * time.Millisecond,
		CleanupInterval: 0,
	}

	cache, _ := Open[string, string](cfg,
		WithClock[string, string](mockClock),
	)
	defer cache.Close()

	cache.Put("key1", "initial")
	mockClock.Advance(60 * time.Millisecond)

	val, ok := cache.Fetch("key1")
	if !ok || val != "initial" {
		t.Fatalf("expected initial, got %v", val)
	}
}

func TestRefreshAhead_NoDoubleRefresh(t *testing.T) {
	mockClock := clock.NewMock(time.Now())
	var loadCount atomic.Int32

	cfg := Config{
		DefaultTTL:      100 * time.Millisecond,
		RefreshAfter:    50 * time.Millisecond,
		CleanupInterval: 0,
	}

	loader := LoaderFunc[string, string](func(key string) (string, error) {
		loadCount.Add(1)
		time.Sleep(100 * time.Millisecond)
		return "refreshed", nil
	})

	cache, _ := Open[string, string](cfg,
		WithClock[string, string](mockClock),
		WithLoader[string, string](loader),
	)
	defer cache.Close()

	cache.Put("key1", "initial")
	mockClock.Advance(60 * time.Millisecond)

	cache.Fetch("key1")
	cache.Fetch("key1")
	cache.Fetch("key1")

	time.Sleep(150 * time.Millisecond)

	if loadCount.Load() != 1 {
		t.Fatalf("expected 1 refresh (deduplicated), got %d", loadCount.Load())
	}
}

func TestRefreshAhead_ConfigValidation(t *testing.T) {
	cfg := Config{
		DefaultTTL:   100 * time.Millisecond,
		RefreshAfter: 200 * time.Millisecond,
	}

	_, err := Open[string, string](cfg)
	if err == nil {
		t.Fatal("expected error for RefreshAfter >= DefaultTTL")
	}
}

func TestRefreshAhead_ZeroRefreshAfter(t *testing.T) {
	cfg := Config{
		DefaultTTL:      100 * time.Millisecond,
		RefreshAfter:    0,
		CleanupInterval: 0,
	}

	cache, _ := Open[string, string](cfg)
	defer cache.Close()

	cache.Put("key1", "value")
	val, ok := cache.Fetch("key1")
	if !ok || val != "value" {
		t.Fatal("should work without refresh-ahead")
	}
}
