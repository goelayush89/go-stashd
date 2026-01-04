package eviction

import (
	"testing"
)

func TestCountMinSketch_IncrementAndEstimate(t *testing.T) {
	cms := NewCountMinSketch(256, 4)

	cms.Increment([]byte("key1"))
	cms.Increment([]byte("key1"))
	cms.Increment([]byte("key1"))

	est := cms.Estimate([]byte("key1"))
	if est != 3 {
		t.Errorf("expected estimate 3, got %d", est)
	}
}

func TestCountMinSketch_MaxValue(t *testing.T) {
	cms := NewCountMinSketch(256, 4)

	for i := 0; i < 20; i++ {
		cms.Increment([]byte("hotkey"))
	}

	est := cms.Estimate([]byte("hotkey"))
	if est > 15 {
		t.Errorf("expected max 15, got %d", est)
	}
}

func TestCountMinSketch_Reset(t *testing.T) {
	cms := NewCountMinSketch(256, 4)

	for i := 0; i < 10; i++ {
		cms.Increment([]byte("key"))
	}

	before := cms.Estimate([]byte("key"))
	cms.Reset()
	after := cms.Estimate([]byte("key"))

	if after >= before {
		t.Errorf("expected estimate to decrease after reset, before=%d after=%d", before, after)
	}
}

func TestCountMinSketch_Count(t *testing.T) {
	cms := NewCountMinSketch(256, 4)

	if cms.Count() != 0 {
		t.Error("expected count 0 initially")
	}

	cms.Increment([]byte("key1"))
	cms.Increment([]byte("key2"))
	cms.Increment([]byte("key3"))

	if cms.Count() != 3 {
		t.Errorf("expected count 3, got %d", cms.Count())
	}
}

func TestCountMinSketch_DifferentKeys(t *testing.T) {
	cms := NewCountMinSketch(1024, 4)

	cms.Increment([]byte("apple"))
	cms.Increment([]byte("apple"))
	cms.Increment([]byte("banana"))

	appleEst := cms.Estimate([]byte("apple"))
	bananaEst := cms.Estimate([]byte("banana"))

	if appleEst != 2 {
		t.Errorf("expected apple=2, got %d", appleEst)
	}
	if bananaEst != 1 {
		t.Errorf("expected banana=1, got %d", bananaEst)
	}
}

func TestCountMinSketch_WithSize(t *testing.T) {
	cms1 := NewCountMinSketchWithSize(100)
	cms2 := NewCountMinSketchWithSize(10000)
	cms3 := NewCountMinSketchWithSize(1000000)

	if cms1.cols < 256 {
		t.Error("minimum width should be 256")
	}
	if cms3.cols > 65536 {
		t.Error("maximum width should be 65536")
	}
	_ = cms2
}

func TestCountMinSketch_UnseenKey(t *testing.T) {
	cms := NewCountMinSketch(256, 4)

	est := cms.Estimate([]byte("never_seen"))
	if est != 0 {
		t.Errorf("expected 0 for unseen key, got %d", est)
	}
}

func TestCountMinSketch_ResetClearsCount(t *testing.T) {
	cms := NewCountMinSketch(256, 4)

	for i := 0; i < 100; i++ {
		cms.Increment([]byte("key"))
	}

	cms.Reset()

	if cms.Count() != 0 {
		t.Errorf("expected count 0 after reset, got %d", cms.Count())
	}
}
