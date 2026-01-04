package eviction

import (
	"container/list"
	"fmt"
	"hash/maphash"
	"sync"
	"time"
)

type TinyLFU[K comparable] struct {
	mu            sync.Mutex
	sketch        *CountMinSketch
	windowLRU     *list.List
	windowIndex   map[K]*list.Element
	mainLRU       *list.List
	mainIndex     map[K]*list.Element
	expiryHeap    *expirationHeap[K]
	expiryIndex   map[K]*heapItem[K]
	windowSize    int
	mainSize      int
	maxSize       int
	seed          maphash.Seed
	resetInterval int
}

type tlfuItem[K comparable] struct {
	key       K
	expiresAt time.Time
	freq      uint8
}

func NewTinyLFU[K comparable](maxSize int) *TinyLFU[K] {
	windowSize := maxSize / 100
	if windowSize < 1 {
		windowSize = 1
	}
	mainSize := maxSize - windowSize

	t := &TinyLFU[K]{
		sketch:        NewCountMinSketchWithSize(maxSize),
		windowLRU:     list.New(),
		windowIndex:   make(map[K]*list.Element),
		mainLRU:       list.New(),
		mainIndex:     make(map[K]*list.Element),
		expiryHeap:    &expirationHeap[K]{},
		expiryIndex:   make(map[K]*heapItem[K]),
		windowSize:    windowSize,
		mainSize:      mainSize,
		maxSize:       maxSize,
		seed:          maphash.MakeSeed(),
		resetInterval: maxSize * 10,
	}

	return t
}

func (t *TinyLFU[K]) OnAccess(key K) {
	t.mu.Lock()
	defer t.mu.Unlock()

	keyBytes := t.keyToBytes(key)
	t.sketch.Increment(keyBytes)

	if t.sketch.Count() > uint64(t.resetInterval) {
		t.sketch.Reset()
	}

	if elem, ok := t.windowIndex[key]; ok {
		t.windowLRU.MoveToFront(elem)
		return
	}

	if elem, ok := t.mainIndex[key]; ok {
		t.mainLRU.MoveToFront(elem)
	}
}

func (t *TinyLFU[K]) OnInsert(key K, expiresAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	keyBytes := t.keyToBytes(key)
	freq := t.sketch.Increment(keyBytes)

	if t.sketch.Count() > uint64(t.resetInterval) {
		t.sketch.Reset()
	}

	item := &tlfuItem[K]{key: key, expiresAt: expiresAt, freq: freq}
	elem := t.windowLRU.PushFront(item)
	t.windowIndex[key] = elem

	if !expiresAt.IsZero() {
		heapItem := &heapItem[K]{key: key, expiresAt: expiresAt}
		t.pushHeap(heapItem)
		t.expiryIndex[key] = heapItem
	}

	t.maybeEvictWindow()
}

func (t *TinyLFU[K]) OnUpdate(key K, expiresAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	keyBytes := t.keyToBytes(key)
	t.sketch.Increment(keyBytes)

	if elem, ok := t.windowIndex[key]; ok {
		item := elem.Value.(*tlfuItem[K])
		item.expiresAt = expiresAt
		t.windowLRU.MoveToFront(elem)
	} else if elem, ok := t.mainIndex[key]; ok {
		item := elem.Value.(*tlfuItem[K])
		item.expiresAt = expiresAt
		t.mainLRU.MoveToFront(elem)
	}

	if existing, ok := t.expiryIndex[key]; ok {
		if expiresAt.IsZero() {
			t.removeHeap(existing)
			delete(t.expiryIndex, key)
		} else {
			existing.expiresAt = expiresAt
			t.fixHeap(existing)
		}
	} else if !expiresAt.IsZero() {
		heapItem := &heapItem[K]{key: key, expiresAt: expiresAt}
		t.pushHeap(heapItem)
		t.expiryIndex[key] = heapItem
	}
}

func (t *TinyLFU[K]) OnDelete(key K) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if elem, ok := t.windowIndex[key]; ok {
		t.windowLRU.Remove(elem)
		delete(t.windowIndex, key)
	}

	if elem, ok := t.mainIndex[key]; ok {
		t.mainLRU.Remove(elem)
		delete(t.mainIndex, key)
	}

	if existing, ok := t.expiryIndex[key]; ok {
		t.removeHeap(existing)
		delete(t.expiryIndex, key)
	}
}

func (t *TinyLFU[K]) NextExpired(now time.Time) (key K, expiresAt time.Time, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(*t.expiryHeap) == 0 {
		return key, expiresAt, false
	}

	top := (*t.expiryHeap)[0]
	if now.Before(top.expiresAt) {
		return key, expiresAt, false
	}

	t.popHeap()
	delete(t.expiryIndex, top.key)

	if elem, ok := t.windowIndex[top.key]; ok {
		t.windowLRU.Remove(elem)
		delete(t.windowIndex, top.key)
	}
	if elem, ok := t.mainIndex[top.key]; ok {
		t.mainLRU.Remove(elem)
		delete(t.mainIndex, top.key)
	}

	return top.key, top.expiresAt, true
}

func (t *TinyLFU[K]) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.windowLRU.Len() + t.mainLRU.Len()
}

func (t *TinyLFU[K]) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.sketch = NewCountMinSketchWithSize(t.maxSize)
	t.windowLRU = list.New()
	t.windowIndex = make(map[K]*list.Element)
	t.mainLRU = list.New()
	t.mainIndex = make(map[K]*list.Element)
	t.expiryHeap = &expirationHeap[K]{}
	t.expiryIndex = make(map[K]*heapItem[K])
}

func (t *TinyLFU[K]) Victim() (key K, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mainLRU.Len() > 0 {
		elem := t.mainLRU.Back()
		if elem != nil {
			item := elem.Value.(*tlfuItem[K])
			return item.key, true
		}
	}

	if t.windowLRU.Len() > 0 {
		elem := t.windowLRU.Back()
		if elem != nil {
			item := elem.Value.(*tlfuItem[K])
			return item.key, true
		}
	}

	return key, false
}

func (t *TinyLFU[K]) maybeEvictWindow() {
	for t.windowLRU.Len() > t.windowSize {
		elem := t.windowLRU.Back()
		if elem == nil {
			break
		}

		candidate := elem.Value.(*tlfuItem[K])
		t.windowLRU.Remove(elem)
		delete(t.windowIndex, candidate.key)

		if t.mainLRU.Len() < t.mainSize {
			newElem := t.mainLRU.PushFront(candidate)
			t.mainIndex[candidate.key] = newElem
		} else {
			victim := t.mainLRU.Back()
			if victim == nil {
				continue
			}
			victimItem := victim.Value.(*tlfuItem[K])

			candidateFreq := t.sketch.Estimate(t.keyToBytes(candidate.key))
			victimFreq := t.sketch.Estimate(t.keyToBytes(victimItem.key))

			if candidateFreq > victimFreq {
				t.mainLRU.Remove(victim)
				delete(t.mainIndex, victimItem.key)
				if existing, ok := t.expiryIndex[victimItem.key]; ok {
					t.removeHeap(existing)
					delete(t.expiryIndex, victimItem.key)
				}

				newElem := t.mainLRU.PushFront(candidate)
				t.mainIndex[candidate.key] = newElem
			} else {
				if existing, ok := t.expiryIndex[candidate.key]; ok {
					t.removeHeap(existing)
					delete(t.expiryIndex, candidate.key)
				}
			}
		}
	}
}

func (t *TinyLFU[K]) keyToBytes(key K) []byte {
	var h maphash.Hash
	h.SetSeed(t.seed)
	switch k := any(key).(type) {
	case string:
		h.WriteString(k)
	case []byte:
		h.Write(k)
	default:
		h.WriteString(fmt.Sprintf("%v", key))
	}
	return h.Sum(nil)
}

func (t *TinyLFU[K]) pushHeap(item *heapItem[K]) {
	item.index = len(*t.expiryHeap)
	*t.expiryHeap = append(*t.expiryHeap, item)
	t.heapUp(len(*t.expiryHeap) - 1)
}

func (t *TinyLFU[K]) popHeap() *heapItem[K] {
	n := len(*t.expiryHeap)
	if n == 0 {
		return nil
	}
	item := (*t.expiryHeap)[0]
	(*t.expiryHeap)[0] = (*t.expiryHeap)[n-1]
	(*t.expiryHeap)[0].index = 0
	(*t.expiryHeap)[n-1] = nil
	*t.expiryHeap = (*t.expiryHeap)[:n-1]
	if len(*t.expiryHeap) > 0 {
		t.heapDown(0)
	}
	item.index = -1
	return item
}

func (t *TinyLFU[K]) removeHeap(item *heapItem[K]) {
	idx := item.index
	if idx < 0 || idx >= len(*t.expiryHeap) {
		return
	}
	n := len(*t.expiryHeap)
	if idx == n-1 {
		(*t.expiryHeap)[n-1] = nil
		*t.expiryHeap = (*t.expiryHeap)[:n-1]
		item.index = -1
		return
	}
	(*t.expiryHeap)[idx] = (*t.expiryHeap)[n-1]
	(*t.expiryHeap)[idx].index = idx
	(*t.expiryHeap)[n-1] = nil
	*t.expiryHeap = (*t.expiryHeap)[:n-1]
	t.fixHeap((*t.expiryHeap)[idx])
	item.index = -1
}

func (t *TinyLFU[K]) fixHeap(item *heapItem[K]) {
	idx := item.index
	if idx < 0 || idx >= len(*t.expiryHeap) {
		return
	}
	t.heapUp(idx)
	t.heapDown(idx)
}

func (t *TinyLFU[K]) heapUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if (*t.expiryHeap)[parent].expiresAt.Before((*t.expiryHeap)[idx].expiresAt) {
			break
		}
		(*t.expiryHeap)[parent], (*t.expiryHeap)[idx] = (*t.expiryHeap)[idx], (*t.expiryHeap)[parent]
		(*t.expiryHeap)[parent].index = parent
		(*t.expiryHeap)[idx].index = idx
		idx = parent
	}
}

func (t *TinyLFU[K]) heapDown(idx int) {
	n := len(*t.expiryHeap)
	for {
		left := 2*idx + 1
		right := 2*idx + 2
		smallest := idx

		if left < n && (*t.expiryHeap)[left].expiresAt.Before((*t.expiryHeap)[smallest].expiresAt) {
			smallest = left
		}
		if right < n && (*t.expiryHeap)[right].expiresAt.Before((*t.expiryHeap)[smallest].expiresAt) {
			smallest = right
		}

		if smallest == idx {
			break
		}

		(*t.expiryHeap)[idx], (*t.expiryHeap)[smallest] = (*t.expiryHeap)[smallest], (*t.expiryHeap)[idx]
		(*t.expiryHeap)[idx].index = idx
		(*t.expiryHeap)[smallest].index = smallest
		idx = smallest
	}
}

var _ Policy[string] = (*TinyLFU[string])(nil)
