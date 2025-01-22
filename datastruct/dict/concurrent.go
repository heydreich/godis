package dict

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

type ConcurrentDict struct {
	table []*Shard
	count int32
}
type Shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	} else {
		return int(n + 1)
	}
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{m: make(map[string]interface{})}
	}
	d := &ConcurrentDict{
		count: 0,
		table: table,
	}
	return d
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(dict.table))
	return (tableSize - 1) & uint32(hashCode)
}

func (dict *ConcurrentDict) getShard(index uint32) *Shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

func (dict *ConcurrentDict) Get(key string) (interface{}, bool) {
	if dict == nil {
		panic("dict is nil")
	}
	var mshard = dict.getShard(dict.spread(fnv32(key)))
	mshard.mutex.RLock()
	defer mshard.mutex.RUnlock()
	val, ok := mshard.m[key]
	return val, ok
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) Put(key string, val interface{}) int {
	if dict == nil {
		panic("dict is nil")
	}
	var mshard = dict.getShard(dict.spread(fnv32(key)))
	mshard.mutex.Lock()
	defer mshard.mutex.Unlock()

	if _, ok := mshard.m[key]; ok {
		mshard.m[key] = val
		return 0
	} else {
		mshard.m[key] = val
		dict.addCount()
		return 1
	}
}

func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

type Locks struct {
	table []*sync.RWMutex
}

func Make(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{table: table}
}

func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("locks is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & hashCode
}

func (locks *Locks) Lock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}

func (locks *Locks) Unlock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]bool)
	for _, key := range keys {
		index := locks.spread(fnv32(key))
		indexMap[index] = true
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		} else {
			return indices[i] > indices[j]
		}
	})
	return indices
}

func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	writeIndices := locks.toLockIndices(writeKeys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, idx := range writeIndices {
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}
