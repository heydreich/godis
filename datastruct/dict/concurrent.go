package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
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
	if shardCount == 1 {
		table := []*Shard{
			{
				m: make(map[string]interface{}),
			},
		}
		return &ConcurrentDict{
			count: 0,
			table: table,
		}
	}
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

func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) int {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(fnv32(key))
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) int {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(fnv32(key))
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}
func (dict *ConcurrentDict) Remove(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(fnv32(key))
	s := dict.getShard(index)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if value, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return value, 1
	}
	return nil, 0
}

// ForEach 方法施加到所有的 kv 元素
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, s := range dict.table {
		s.mutex.RLock()
		for key, value := range s.m {
			if !consumer(key, value) {
				break
			}
		}
	}
}
func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, 0, dict.Len())
	dict.ForEach(func(key string, val interface{}) bool {
		keys = append(keys, key)
		return true
	})

	return keys
}
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	if dict == nil {
		panic("dict is nil")
	}
	keys := make([]string, 0, limit)
	shardCount := len(dict.table)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		s := dict.getShard(uint32(nR.Intn(shardCount)))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			keys = append(keys, key)
			i++
		}
	}
	return keys
}

func (s *Shard) RandomKey() string {
	if s == nil {
		panic("shard is nil")
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for key := range s.m {
		return key
	}

	return ""
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	if dict == nil {
		panic("dict is nil")
	}
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	keys := make(map[string]struct{})
	shardCount := len(dict.table)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(keys) != limit {
		s := dict.getShard(uint32(nR.Intn(shardCount)))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			if _, exists := keys[key]; !exists {
				keys[key] = struct{}{}
			}
		}
	}
	arr := make([]string, 0, limit)
	for key := range keys {
		arr = append(arr, key)
	}
	return arr
}
func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(len(dict.table))
}
