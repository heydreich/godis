package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashFunc func([]byte) uint32

type Map struct {
	hashFunc HashFunc // 哈希函数
	replicas int      // 虚拟节点
	nodes    []string
	keys     []int
	hashMap  map[int]string // 保存哈希值与实际节点的映射关系
}

func New(replicas int, fn HashFunc) *Map {
	hashFunc := crc32.ChecksumIEEE
	if fn != nil {
		hashFunc = fn
	}

	return &Map{
		hashFunc: hashFunc,
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
}

func (m *Map) IsEmpty() bool {
	return m.keys == nil || len(m.keys) == 0
}

func (m *Map) AddNodes(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}

		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFunc([]byte(key + "-" + strconv.Itoa(i))))
			m.keys = append(m.keys, hash)
			//记录在hash环里
			m.hashMap[hash] = key
		}
		m.nodes = append(m.nodes, key)
	}
	sort.Ints(m.keys)
}

func (m *Map) PickNode(key string) (string, bool) {
	if m.IsEmpty() {
		return "", false
	}

	hash := int(m.hashFunc([]byte(key)))

	index := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	if index == len(m.keys) {
		index = 0
	}
	node := m.keys[index]
	return m.hashMap[node], true
}

func (m *Map) GetAllNodes() []string {
	return m.nodes
}
