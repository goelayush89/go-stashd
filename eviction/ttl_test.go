package eviction

import (
	"testing"
	"time"
)

func TestTTL_OnInsert(t *testing.T) {
	ttl := NewTTL[string]()

	ttl.OnInsert("key1", time.Now().Add(1*time.Hour))
	ttl.OnInsert("key2", time.Now().Add(2*time.Hour))
	ttl.OnInsert("key3", time.Time{}) // no expiry

	if ttl.Len() != 2 {
		t.Errorf("expected 2 items in heap, got %d", ttl.Len())
	}
}

func TestTTL_OnUpdate(t *testing.T) {
	ttl := NewTTL[string]()
	now := time.Now()

	ttl.OnInsert("key1", now.Add(1*time.Hour))
	ttl.OnUpdate("key1", now.Add(30*time.Minute))

	key, expiresAt, ok := ttl.NextExpired(now.Add(45 * time.Minute))
	if !ok {
		t.Fatal("expected expired item")
	}
	if key != "key1" {
		t.Errorf("expected key1, got %s", key)
	}
	if !expiresAt.Equal(now.Add(30 * time.Minute)) {
		t.Error("expiry time not updated correctly")
	}
}

func TestTTL_OnDelete(t *testing.T) {
	ttl := NewTTL[string]()

	ttl.OnInsert("key1", time.Now().Add(1*time.Hour))
	ttl.OnInsert("key2", time.Now().Add(2*time.Hour))

	ttl.OnDelete("key1")

	if ttl.Len() != 1 {
		t.Errorf("expected 1 item after delete, got %d", ttl.Len())
	}
}

func TestTTL_NextExpired(t *testing.T) {
	ttl := NewTTL[string]()
	now := time.Now()

	ttl.OnInsert("key1", now.Add(-1*time.Hour))
	ttl.OnInsert("key2", now.Add(1*time.Hour))

	key, _, ok := ttl.NextExpired(now)
	if !ok || key != "key1" {
		t.Error("expected key1 to be expired")
	}

	_, _, ok = ttl.NextExpired(now)
	if ok {
		t.Error("key2 should not be expired yet")
	}
}

func TestTTL_Clear(t *testing.T) {
	ttl := NewTTL[string]()

	for i := 0; i < 10; i++ {
		ttl.OnInsert("key", time.Now().Add(time.Hour))
	}

	ttl.Clear()

	if ttl.Len() != 0 {
		t.Errorf("expected 0 after clear, got %d", ttl.Len())
	}
}

func TestTTL_OnUpdateWithZeroTime(t *testing.T) {
	ttl := NewTTL[string]()
	now := time.Now()

	ttl.OnInsert("key1", now.Add(1*time.Hour))
	ttl.OnUpdate("key1", time.Time{})

	if ttl.Len() != 0 {
		t.Errorf("expected 0 items after removing expiry, got %d", ttl.Len())
	}
}

func TestTTL_OnUpdateNonExistent(t *testing.T) {
	ttl := NewTTL[string]()
	now := time.Now()

	ttl.OnUpdate("nonexistent", now.Add(1*time.Hour))

	if ttl.Len() != 1 {
		t.Errorf("expected 1 item after update-insert, got %d", ttl.Len())
	}
}

func TestTTL_NextExpiredEmpty(t *testing.T) {
	ttl := NewTTL[string]()

	_, _, ok := ttl.NextExpired(time.Now())
	if ok {
		t.Error("expected no expired item from empty heap")
	}
}

func TestTTL_HeapOrdering(t *testing.T) {
	ttl := NewTTL[string]()
	now := time.Now()

	ttl.OnInsert("key3", now.Add(3*time.Hour))
	ttl.OnInsert("key1", now.Add(1*time.Hour))
	ttl.OnInsert("key2", now.Add(2*time.Hour))

	future := now.Add(4 * time.Hour)

	key, _, _ := ttl.NextExpired(future)
	if key != "key1" {
		t.Errorf("expected key1 first, got %s", key)
	}

	key, _, _ = ttl.NextExpired(future)
	if key != "key2" {
		t.Errorf("expected key2 second, got %s", key)
	}

	key, _, _ = ttl.NextExpired(future)
	if key != "key3" {
		t.Errorf("expected key3 third, got %s", key)
	}
}
