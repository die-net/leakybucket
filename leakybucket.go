package leakybucket

import (
	"hash/fnv"
	"sync"
	"time"
)

const (
	// DrainPerSecond is the number of tokens per second drained from each bucket
	DrainPerSecond      = 1000000000
	gcScanEntries       = 1000
	gcMustRemoveEntries = 100
)

type entry struct {
	tokens  int64 // one billion drained per second
	updated int64 // unix nanoseconds
}

// Cache is a size-limited hash-map implementation of the leaky
// bucket rate-limiting algorithm. Entries are roughly 78 bytes each.
type Cache struct {
	MaxEntries int

	mu      sync.Mutex
	cache   map[uint64]entry
	entries int
}

// New creates a Cache capable of storing up to maxEntries entries.
func New(maxEntries int) *Cache {
	if maxEntries <= 0 {
		return nil
	}

	b := &Cache{
		MaxEntries: maxEntries,
		cache:      make(map[uint64]entry),
	}

	return b
}

// Put attempts to add quantity tokens into the bucket referred to by a
// uint64 key that has a size limit of limit tokens.  Return the current
// number of tokens in the bucket, whether the key already existed, and
// whether there was enough room in the bucket to add the requested number
// of tokens.
func (lb *Cache) Put(key uint64, quantity, limit int64) (int64, bool, bool) {
	now := time.Now().UnixNano()

	lb.mu.Lock()
	tokens, exists, ok := lb.put(key, quantity, limit, now)
	lb.mu.Unlock()

	return tokens, exists, ok
}

// PutString attempt to add quantity tokens into the bucket referred to the
// string ks that has a size limit of limit tokens.  Return the current
// number of tokens in the bucket, whether the key already existed, and
// whether there was enough room in the bucket to add the requested number
// of tokens.
func (lb *Cache) PutString(ks string, quantity, limit int64) (int64, bool, bool) {
	k := key(ks)
	now := time.Now().UnixNano()

	lb.mu.Lock()
	tokens, exists, ok := lb.put(k, quantity, limit, now)
	lb.mu.Unlock()

	return tokens, exists, ok
}

func key(k string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(k))
	return h.Sum64()
}

func (lb *Cache) put(k uint64, quantity, limit, now int64) (int64, bool, bool) {
	e, exists := lb.cache[k]
	if exists {
		e = e.update(now)
	} else {
		e = entry{tokens: 0, updated: now}
	}

	ok := e.tokens+quantity <= limit
	if ok {
		e.tokens += quantity
	}
	lb.cache[k] = e

	if !exists {
		lb.entries++
		if lb.entries > lb.MaxEntries {
			lb.gc(now)
		}
	}

	return e.tokens, exists, ok
}

// gc frees up space in the map, forcibly if necessary.
func (lb *Cache) gc(now int64) {
	left := lb.entries - (lb.MaxEntries - gcMustRemoveEntries)

	// Try freeing up entries in a random part of the map.
	left -= lb.scan(now, gcScanEntries)
	if left <= 0 {
		return
	}

	// If that failed, try one more time in a different part of the map.
	left -= lb.scan(now, gcScanEntries)
	if left <= 0 {
		return
	}

	// If that failed, just delete some keys.
	for k := range lb.cache {
		delete(lb.cache, k)
		lb.entries--
		left--
		if left <= 0 {
			return
		}
	}
}

// scan attempts to find buckets which are empty to delete.
func (lb *Cache) scan(now int64, count int) int {
	start := lb.entries

	n := 0
	for k, e := range lb.cache {
		e = e.update(now)

		if e.tokens <= 0 {
			delete(lb.cache, k)
			lb.entries--
		} else {
			lb.cache[k] = e
		}

		n++
		if n >= count {
			break
		}
	}

	return lb.entries - start
}

// update drains a bucket according to how much time as elapsed since last update.
func (e entry) update(now int64) entry {
	// How many nanoseconds have elapsed since last update?
	s := now - e.updated
	if s < 0 {
		s = 0
	}

	// Drain one token per elapsed nanosecond.
	t := e.tokens - s
	if t < 0 {
		t = 0
	}

	return entry{tokens: t, updated: now}
}
