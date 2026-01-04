package storage

type Memory[K comparable, V any] struct {
	items map[K]V
}

func NewMemory[K comparable, V any]() *Memory[K, V] {
	return &Memory[K, V]{items: make(map[K]V)}
}

func (m *Memory[K, V]) Load(key K) (V, bool) {
	value, ok := m.items[key]
	return value, ok
}

func (m *Memory[K, V]) Store(key K, value V) {
	m.items[key] = value
}

func (m *Memory[K, V]) Delete(key K) bool {
	_, existed := m.items[key]
	delete(m.items, key)
	return existed
}

func (m *Memory[K, V]) Range(fn func(key K, value V) bool) {
	for k, v := range m.items {
		if !fn(k, v) {
			return
		}
	}
}

func (m *Memory[K, V]) Len() int {
	return len(m.items)
}

func (m *Memory[K, V]) Clear() {
	m.items = make(map[K]V)
}

func (m *Memory[K, V]) Close() error { return nil }
