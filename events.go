package stashd

type EvictionReason uint8

const (
	EvictionReasonDeleted EvictionReason = iota + 1
	EvictionReasonExpired
	EvictionReasonCapacity
)

func (r EvictionReason) String() string {
	switch r {
	case EvictionReasonDeleted:
		return "deleted"
	case EvictionReasonExpired:
		return "expired"
	case EvictionReasonCapacity:
		return "capacity"
	default:
		return "unknown"
	}
}

type EventHandlers[K comparable, V any] struct {
	OnPut      func(key K, value V)
	OnUpdate   func(key K, oldValue V, newValue V)
	OnEviction func(reason EvictionReason, key K, value V)
	OnHit      func(key K, value V)
}
