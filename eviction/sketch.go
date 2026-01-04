package eviction

import (
	"hash/maphash"
	"math"
	"sync/atomic"
)

type CountMinSketch struct {
	rows    int
	cols    int
	matrix  [][]uint8
	seeds   []maphash.Seed
	counter atomic.Uint64
}

func NewCountMinSketch(width, depth int) *CountMinSketch {
	matrix := make([][]uint8, depth)
	seeds := make([]maphash.Seed, depth)
	for i := 0; i < depth; i++ {
		matrix[i] = make([]uint8, width)
		seeds[i] = maphash.MakeSeed()
	}
	return &CountMinSketch{
		rows:   depth,
		cols:   width,
		matrix: matrix,
		seeds:  seeds,
	}
}

func NewCountMinSketchWithSize(expectedItems int) *CountMinSketch {
	width := int(math.Ceil(float64(expectedItems) / 4))
	if width < 256 {
		width = 256
	}
	if width > 65536 {
		width = 65536
	}
	return NewCountMinSketch(width, 4)
}

func (c *CountMinSketch) Increment(key []byte) uint8 {
	c.counter.Add(1)
	min := uint8(255)
	for i := 0; i < c.rows; i++ {
		idx := c.hash(i, key) % uint64(c.cols)
		val := c.matrix[i][idx]
		if val < 15 {
			c.matrix[i][idx] = val + 1
			val++
		}
		if val < min {
			min = val
		}
	}
	return min
}

func (c *CountMinSketch) Estimate(key []byte) uint8 {
	min := uint8(255)
	for i := 0; i < c.rows; i++ {
		idx := c.hash(i, key) % uint64(c.cols)
		val := c.matrix[i][idx]
		if val < min {
			min = val
		}
	}
	return min
}

func (c *CountMinSketch) Reset() {
	for i := 0; i < c.rows; i++ {
		for j := 0; j < c.cols; j++ {
			c.matrix[i][j] = c.matrix[i][j] / 2
		}
	}
	c.counter.Store(0)
}

func (c *CountMinSketch) Count() uint64 {
	return c.counter.Load()
}

func (c *CountMinSketch) hash(row int, key []byte) uint64 {
	var h maphash.Hash
	h.SetSeed(c.seeds[row])
	h.Write(key)
	return h.Sum64()
}
