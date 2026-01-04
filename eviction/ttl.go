package eviction

import (
	"container/heap"
	"sync"
	"time"
)

type TTL[K comparable] struct {
	mu    sync.Mutex
	heap  *expirationHeap[K]
	index map[K]*heapItem[K]
}

func NewTTL[K comparable]() *TTL[K] {
	h := &expirationHeap[K]{}
	heap.Init(h)
	return &TTL[K]{
		heap:  h,
		index: make(map[K]*heapItem[K]),
	}
}

func (t *TTL[K]) OnAccess(key K) {}

func (t *TTL[K]) OnInsert(key K, expiresAt time.Time) {
	if expiresAt.IsZero() {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if existing, ok := t.index[key]; ok {
		existing.expiresAt = expiresAt
		heap.Fix(t.heap, existing.index)
		return
	}

	item := &heapItem[K]{key: key, expiresAt: expiresAt}
	heap.Push(t.heap, item)
	t.index[key] = item
}

func (t *TTL[K]) OnUpdate(key K, expiresAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if existing, ok := t.index[key]; ok {
		if expiresAt.IsZero() {
			heap.Remove(t.heap, existing.index)
			delete(t.index, key)
			return
		}
		existing.expiresAt = expiresAt
		heap.Fix(t.heap, existing.index)
		return
	}

	if expiresAt.IsZero() {
		return
	}

	item := &heapItem[K]{key: key, expiresAt: expiresAt}
	heap.Push(t.heap, item)
	t.index[key] = item
}

func (t *TTL[K]) OnDelete(key K) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if existing, ok := t.index[key]; ok {
		heap.Remove(t.heap, existing.index)
		delete(t.index, key)
	}
}

func (t *TTL[K]) NextExpired(now time.Time) (key K, expiresAt time.Time, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.heap.Len() == 0 {
		return key, expiresAt, false
	}

	item := (*t.heap)[0]
	if now.Before(item.expiresAt) {
		return key, expiresAt, false
	}

	heap.Pop(t.heap)
	delete(t.index, item.key)

	return item.key, item.expiresAt, true
}

func (t *TTL[K]) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.heap.Len()
}

func (t *TTL[K]) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.heap = &expirationHeap[K]{}
	heap.Init(t.heap)
	t.index = make(map[K]*heapItem[K])
}

func (t *TTL[K]) Victim() (key K, ok bool) {
	return key, false
}

type heapItem[K comparable] struct {
	key       K
	expiresAt time.Time
	index     int
}

type expirationHeap[K comparable] []*heapItem[K]

func (h expirationHeap[K]) Len() int           { return len(h) }
func (h expirationHeap[K]) Less(i, j int) bool { return h[i].expiresAt.Before(h[j].expiresAt) }
func (h expirationHeap[K]) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }

func (h *expirationHeap[K]) Push(x any) {
	item := x.(*heapItem[K])
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *expirationHeap[K]) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

var _ Policy[string] = (*TTL[string])(nil)
