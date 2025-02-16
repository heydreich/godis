package utils

import (
	"godis/datastruct/dict"
	List "godis/datastruct/list"
	"godis/datastruct/set"
	"godis/datastruct/sortedset"
	"godis/interface/database"
	"godis/redis/protocol"
	"strconv"
	"time"
)

var (
	setCmd       = []byte("SET")
	zAddCmd      = []byte("ZADD")
	hSetCmd      = []byte("HSET")
	sAddCmd      = []byte("SADD")
	rPushCmd     = []byte("RPUSH")
	pExpireAtCmd = []byte("PEXPIREAT")
)

// ExpireToBytes 将expireAt命令转为[]byte(*reply.MultiBulkStringReply.ToBytes())
func ExpireToBytes(key string, expireAt time.Time) []byte {
	return ExpireToReply(key, expireAt).ToBytes()
}

// ExpireToCmdLine 将expireAt命令转为[][]byte
func ExpireToCmdLine(key string, expireAt time.Time) [][]byte {
	return ExpireToReply(key, expireAt).Args
}

// ExpireToReply 将expireAt命令转为*reply.MultiBulkStringReply
func ExpireToReply(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtCmd
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))

	return protocol.MakeMultiBulkReply(args)
}

// EntityToBytes serialize data entity to redis multi bulk bytes
func EntityToBytes(key string, entity *database.DataEntity) []byte {
	if entity == nil {
		return nil
	}

	return EntityToReply(key, entity).ToBytes()
}

func EntityToCmdLine(key string, entity *database.DataEntity) [][]byte {
	if entity == nil {
		return nil
	}

	return EntityToReply(key, entity).Args
}

func EntityToReply(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *protocol.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
	case List.List:
		cmd = listToCmd(key, val)
	case set.Set:
		cmd = setToCmd(key, val)
	case dict.Dict:
		cmd = hashToCmd(key, val)
	case *sortedset.SortedSet:
		cmd = zSetToCmd(key, val)
	}
	if cmd == nil {
		return nil
	}
	return cmd
}

func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes

	return protocol.MakeMultiBulkReply(args)
}

func listToCmd(key string, list List.List) *protocol.MultiBulkReply {
	args := make([][]byte, 2, 2+list.Len())
	args[0] = rPushCmd
	args[1] = []byte(key)

	list.ForEach(func(i int, v interface{}) bool {
		bytes, _ := v.([]byte)
		args = append(args, bytes)

		return true
	})

	return protocol.MakeMultiBulkReply(args)
}

func setToCmd(key string, set set.Set) *protocol.MultiBulkReply {
	args := make([][]byte, 2, 2+set.Len())
	args[0] = sAddCmd
	args[1] = []byte(key)

	set.ForEach(func(member string) bool {
		args = append(args, []byte(member))
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

func hashToCmd(key string, dict dict.Dict) *protocol.MultiBulkReply {
	args := make([][]byte, 2, 2+2*dict.Len())
	args[0] = hSetCmd
	args[1] = []byte(key)
	dict.ForEach(func(key string, val interface{}) bool {
		bytes, _ := val.([]byte)
		args = append(args, []byte(key))
		args = append(args, bytes)
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

func zSetToCmd(key string, zset *sortedset.SortedSet) *protocol.MultiBulkReply {
	args := make([][]byte, 2, 2+2*zset.Len())
	args[0] = zAddCmd
	args[1] = []byte(key)
	zset.ForEach(0, zset.Len(), false, func(element *sortedset.Element) bool {
		args = append(args, []byte(strconv.FormatFloat(element.Score, 'f', -1, 64)))
		args = append(args, []byte(element.Member))
		return true
	})

	return protocol.MakeMultiBulkReply(args)
}
