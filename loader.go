package stashd

import (
	"fmt"

	"golang.org/x/sync/singleflight"
)

type Loader[K comparable, V any] interface {
	Load(key K) (V, error)
}

type LoaderFunc[K comparable, V any] func(key K) (V, error)

func (f LoaderFunc[K, V]) Load(key K) (V, error) {
	return f(key)
}

type SuppressedLoader[K comparable, V any] struct {
	loader Loader[K, V]
	group  singleflight.Group
}

func NewSuppressedLoader[K comparable, V any](loader Loader[K, V]) *SuppressedLoader[K, V] {
	return &SuppressedLoader[K, V]{loader: loader}
}

func (l *SuppressedLoader[K, V]) Load(key K) (V, error) {
	strKey := fmt.Sprint(key)

	result, err, _ := l.group.Do(strKey, func() (interface{}, error) {
		return l.loader.Load(key)
	})

	if err != nil {
		var zero V
		return zero, err
	}

	return result.(V), nil
}
