package eviction

import "time"

type Policy[K comparable] interface {
	OnAccess(key K)
	OnInsert(key K, expiresAt time.Time)
	OnUpdate(key K, expiresAt time.Time)
	OnDelete(key K)
	NextExpired(now time.Time) (key K, expiresAt time.Time, ok bool)
	Victim() (key K, ok bool)
	Len() int
	Clear()
}
