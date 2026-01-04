package eviction

import (
	"testing"
	"time"
)

func TestTinyLFU_OnInsert(t *testing.T) {
	lfu := NewTinyLFU[string](100)

	lfu.OnInsert("key1", time.Now().Add(1*time.Hour))
	lfu.OnInsert("key2", time.Now().Add(2*time.Hour))

	if lfu.Len() != 2 {
		t.Errorf("expected 2 items, got %d", lfu.Len())
	}
}

func TestTinyLFU_OnAccess(t *testing.T) {
	lfu := NewTinyLFU[string](100)

	lfu.OnInsert("key1", time.Now().Add(1*time.Hour))

	for i := 0; i < 10; i++ {
		lfu.OnAccess("key1")
	}
}

func TestTinyLFU_OnDelete(t *testing.T) {
	lfu := NewTinyLFU[string](100)

	lfu.OnInsert("key1", time.Now().Add(1*time.Hour))
	lfu.OnInsert("key2", time.Now().Add(1*time.Hour))

	lfu.OnDelete("key1")

	if lfu.Len() != 1 {
		t.Errorf("expected 1 item after delete, got %d", lfu.Len())
	}
}

func TestTinyLFU_Victim(t *testing.T) {
	lfu := NewTinyLFU[string](10)

	for i := 0; i < 10; i++ {
		lfu.OnInsert("key", time.Now().Add(1*time.Hour))
	}

	key, ok := lfu.Victim()
	if !ok {
		t.Error("expected a victim")
	}
	if key == "" {
		t.Error("victim key should not be empty")
	}
}

func TestTinyLFU_Clear(t *testing.T) {
	lfu := NewTinyLFU[string](100)

	for i := 0; i < 50; i++ {
		lfu.OnInsert("key", time.Now().Add(1*time.Hour))
	}

	lfu.Clear()

	if lfu.Len() != 0 {
		t.Errorf("expected 0 after clear, got %d", lfu.Len())
	}
}

func TestTinyLFU_NextExpired(t *testing.T) {
	lfu := NewTinyLFU[string](100)
	now := time.Now()

	lfu.OnInsert("expired", now.Add(-1*time.Hour))
	lfu.OnInsert("valid", now.Add(1*time.Hour))

	key, _, ok := lfu.NextExpired(now)
	if !ok || key != "expired" {
		t.Error("expected expired key")
	}

	_, _, ok = lfu.NextExpired(now)
	if ok {
		t.Error("valid key should not be expired")
	}
}

func TestTinyLFU_WindowToMainPromotion(t *testing.T) {
	lfu := NewTinyLFU[string](10)

	for i := 0; i < 3; i++ {
		lfu.OnInsert("key", time.Now().Add(1*time.Hour))
	}

	if lfu.Len() > 10 {
		t.Error("should not exceed max size")
	}
}

func TestTinyLFU_FrequencyBasedEviction(t *testing.T) {
	lfu := NewTinyLFU[string](20)

	lfu.OnInsert("hot", time.Time{})
	for i := 0; i < 50; i++ {
		lfu.OnAccess("hot")
	}

	for i := 0; i < 25; i++ {
		lfu.OnInsert("cold", time.Time{})
	}
}

func TestTinyLFU_VictimEmpty(t *testing.T) {
	lfu := NewTinyLFU[string](100)

	_, ok := lfu.Victim()
	if ok {
		t.Error("expected no victim from empty cache")
	}
}

func TestTinyLFU_OnUpdate(t *testing.T) {
	lfu := NewTinyLFU[string](100)
	now := time.Now()

	lfu.OnInsert("key1", now.Add(1*time.Hour))
	lfu.OnUpdate("key1", now.Add(30*time.Minute))
}

func TestTinyLFU_IntKeys(t *testing.T) {
	lfu := NewTinyLFU[int](100)

	lfu.OnInsert(1, time.Now().Add(1*time.Hour))
	lfu.OnInsert(2, time.Now().Add(1*time.Hour))
	lfu.OnAccess(1)

	if lfu.Len() != 2 {
		t.Errorf("expected 2 items, got %d", lfu.Len())
	}
}
