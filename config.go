package stashd

import (
	"fmt"
	"time"
)

type EvictionPolicy string

const (
	EvictionPolicyLRU     EvictionPolicy = "lru"
	EvictionPolicyTinyLFU EvictionPolicy = "tinylfu"
)

type Config struct {
	DefaultTTL      time.Duration
	CleanupInterval time.Duration
	MaxEntries      int
	WALPath         string
	SyncWrites      bool
	EvictionPolicy  EvictionPolicy
	RefreshAfter    time.Duration
}

func DefaultConfig() Config {
	return Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 1 * time.Minute,
		MaxEntries:      0,
		WALPath:         "",
		SyncWrites:      false,
		EvictionPolicy:  EvictionPolicyLRU,
		RefreshAfter:    0,
	}
}

func (c Config) Validate() error {
	if c.DefaultTTL < 0 {
		return fmt.Errorf("DefaultTTL cannot be negative")
	}
	if c.CleanupInterval < 0 {
		return fmt.Errorf("CleanupInterval cannot be negative")
	}
	if c.MaxEntries < 0 {
		return fmt.Errorf("MaxEntries cannot be negative")
	}
	if c.EvictionPolicy == EvictionPolicyTinyLFU && c.MaxEntries <= 0 {
		return fmt.Errorf("MaxEntries must be set when using TinyLFU eviction policy")
	}
	if c.RefreshAfter < 0 {
		return fmt.Errorf("RefreshAfter cannot be negative")
	}
	if c.RefreshAfter > 0 && c.DefaultTTL > 0 && c.RefreshAfter >= c.DefaultTTL {
		return fmt.Errorf("RefreshAfter must be less than DefaultTTL")
	}
	return nil
}
