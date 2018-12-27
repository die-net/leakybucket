package leakybucket

import (
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var entries = []struct {
	key   uint64
	value int64
}{
	{1, 1000},
	{2, 2000},
	{3, 30000},
	{4, 400000},
	{5, 50000000},
}

func TestPut(t *testing.T) {
	b := New(10000)

	now := time.Now().UnixNano()

	// Add the test entries
	for _, e := range entries {
		tokens, exists, ok := b.put(e.key, e.value, 1000000000, now)
		assert.Equal(t, e.value, tokens)
		assert.False(t, exists)
		assert.True(t, ok)
	}

	assert.Equal(t, 5, b.entries)

	// Fast-forward time by 500ns, gc, and verify that the entries have
	// reasonable values.
	now += 500
	b.gc(now)

	for _, e := range entries {
		tokens, exists, ok := b.put(e.key, 1, 1000000000, now)
		assert.Equal(t, e.value-500+1, tokens)
		assert.True(t, exists)
		assert.True(t, ok)
	}

	// Fast-forward time by another 2000ns, gc, and check that we've
	// freed 2 entries.
	now += 2000
	b.gc(now)
	assert.Equal(t, 3, b.entries)
}

func TestPutString(t *testing.T) {
	b := New(10000)

	_, _, _ = b.PutString("1", 1000, 1000000000)
	_, _, _ = b.PutString("2", 2000, 1000000000)
	_, _, _ = b.PutString("2", 500, 1000000000)

	assert.Equal(t, 2, b.entries)
}

func TestGC(t *testing.T) {
	b := New(10000)

	now := time.Now().UnixNano()

	// Add one too many entries and make sure we removed gcMustRemoveEntries.
	for n := 0; n < 10001; n++ {
		_, _, _ = b.put(uint64(n), int64(1000+n), 1000000000, now)
	}

	assert.Equal(t, 10000-gcMustRemoveEntries, b.entries)

	// Fast forward 2000ns, add another 200 entries, and make sure we
	// did more GC.
	now += 2000

	for n := 10002; n < 10200; n++ {
		_, _, _ = b.put(uint64(n), int64(1000+n), 1000000000, now)
	}

	assert.InDelta(t, 9600, b.entries, 200)
}

func TestMaxEntries(t *testing.T) {
	c := New(2)

	for _, e := range entries {
		_, _, _ = c.Put(e.key, e.value, 100000000)
	}

	// Make sure only the last two entries were kept.
	assert.Equal(t, 2, c.entries)
}

func TestRace(t *testing.T) {
	c := New(4000)

	for worker := 0; worker < 8; worker++ {
		go testRaceWorker(c)
	}
}

func testRaceWorker(c *Cache) {
	for n := 0; n < 1000; n++ {
		_, _, _ = c.Put(uint64(rand.Int31n(100)), rand.Int63n(1000000000), 1000000000)
	}
}

func TestOverhead(t *testing.T) {
	if testing.Short() || !testing.Verbose() {
		t.SkipNow()
	}

	num := 100000
	c := New(num)

	now := time.Now().UnixNano()

	mem := readMem()

	for n := 0; n < num*4; n++ {
		_, _, _ = c.put(uint64(n), int64(n), int64(n), now)
	}

	mem = readMem() - mem
	t.Log("entryOverhead =", mem/int64(num))
}

func readMem() int64 {
	m := runtime.MemStats{}
	runtime.GC()
	runtime.ReadMemStats(&m)
	return int64(m.Alloc)
}
