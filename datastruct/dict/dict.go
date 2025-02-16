package dict

type Consumer func(key string, val interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	// Put 返回存进去了几个
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) int
	PutIfExists(key string, val interface{}) int
	Remove(key string) (val interface{}, result int)
	// ForEach 方法施加到所有的 kv 元素
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
}
