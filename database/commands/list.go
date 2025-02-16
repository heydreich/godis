package commands

import (
	"godis/database/engine"
	List "godis/datastruct/list"
	"godis/interface/database"
	"godis/interface/redis"
	"godis/redis/protocol"
	"strconv"
)

func execLPush(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	values := args[1:]

	list, _, errReply := getOrInitList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	for _, value := range values {
		list.Insert(0, value)
	}

	return protocol.MakeIntReply(int64(list.Len())), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execLPushX(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	value := args[1]

	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if list == nil {
		return protocol.MakeIntReply(0), nil
	}

	list.Insert(0, value)

	return protocol.MakeIntReply(int64(list.Len())), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execRPush(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	values := args[1:]

	list, _, errReply := getOrInitList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	for _, value := range values {
		list.Add(value)
	}

	return protocol.MakeIntReply(int64(list.Len())), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execRPushX(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	value := args[1]

	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if list == nil {
		return protocol.MakeIntReply(0), nil
	}

	list.Add(value)

	return protocol.MakeIntReply(int64(list.Len())), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execLPop(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if list == nil {
		return protocol.MakeNullBulkReply(), nil
	}

	val, _ := list.Remove(0).([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}

	return protocol.MakeBulkReply(val), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execRPop(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if list == nil {
		return protocol.MakeNullBulkReply(), nil
	}

	val, _ := list.RemoveLast().([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}

	return protocol.MakeBulkReply(val), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execLIndex(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR index is not an integer or out of range"), nil
	}

	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if list == nil {
		return protocol.MakeNullBulkReply(), nil
	}

	if index < 0 {
		if list.Len()+index < 0 {
			return protocol.MakeNullBulkReply(), nil
		}

		index = list.Len() + index
	}

	if index < 0 || index >= list.Len() {
		return protocol.MakeNullBulkReply(), nil
	}

	val, _ := list.Get(index).([]byte)

	return protocol.MakeBulkReply(val), nil
}

func execLLen(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if list == nil {
		return protocol.MakeIntReply(0), nil
	}

	return protocol.MakeIntReply(int64(list.Len())), nil
}

func execLRem(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR count is not an integer or out of range"), nil
	}
	value := string(args[2])

	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if list == nil {
		return protocol.MakeIntReply(0), nil
	}

	var result int
	if count == 0 {
		// 删除全部
		result = list.RemoveAllByVal(func(raw interface{}) bool {
			val, _ := raw.([]byte)
			if string(val) == value {
				return true
			}

			return false
		})
	} else if count > 0 {
		// 从前向后删除
		result = list.RemoveByVal(func(raw interface{}) bool {
			val, _ := raw.([]byte)
			if string(val) == value {
				return true
			}

			return false
		}, count)
	} else {
		// 从后向前删除
		result = list.ReverseRemoveByVal(func(raw interface{}) bool {
			val, _ := raw.([]byte)
			if string(val) == value {
				return true
			}

			return false
		}, count)
	}

	if list.Len() == 0 {
		db.Remove(key)
	}

	return protocol.MakeIntReply(int64(result)), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execLTrim(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR start is not an integer or out of range"), nil
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply("ERR stop is not an integer or out of range"), nil
	}

	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if list == nil {
		return protocol.MakeOkReply(), nil
	}

	start, stop = parseStartAndStop(list, start, stop)
	if start > stop {
		return protocol.MakeOkReply(), nil
	}

	for i := start; i < stop; i++ {
		list.Remove(start)
	}

	if list.Len() == 0 {
		db.Remove(key)
	}

	return protocol.MakeOkReply(), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execLRange(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR start is not an integer or out of range"), nil
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply("ERR stop is not an integer or out of range"), nil
	}

	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if list == nil {
		return protocol.MakeOkReply(), nil
	}

	start, stop = parseStartAndStop(list, start, stop)
	if start > stop {
		return protocol.MakeEmptyMultiBulkReply(), nil
	}

	raws := list.Range(start, stop)
	results := make([][]byte, 0, len(raws))
	for _, raw := range raws {
		result, ok := raw.([]byte)
		if !ok {
			continue
		}
		results = append(results, result)
	}

	return protocol.MakeMultiBulkReply(results), nil
}

func execLSet(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR index is not an integer or out of range"), nil
	}
	val := args[2]

	list, errReply := getAsList(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if list == nil {
		return protocol.MakeOkReply(), nil
	}

	if index < 0 {
		if list.Len()-index < 0 {
			return protocol.MakeErrReply("ERR index out of range"), nil
		}
		index = list.Len() + index
	}

	if index >= list.Len() {
		return protocol.MakeErrReply("ERR index out of range"), nil
	}

	list.Set(index, val)

	return protocol.MakeOkReply(), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func getAsList(db *engine.DB, key string) (list List.List, errorReply protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	list, ok := entity.Data.(List.List)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return list, nil
}

func getOrInitList(db *engine.DB, key string) (list List.List, inited bool, errReply protocol.ErrorReply) {
	list, errReply = getAsList(db, key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if list == nil {
		list = List.MakeQuickList()
		db.PutEntity(key, &database.DataEntity{
			Data: list,
		})
		inited = true
	}
	return list, inited, nil
}

func parseStartAndStop(list List.List, rawStart, rawStop int) (int, int) {
	// 处理负数
	start := parseMinus(list, rawStart)
	stop := parseMinus(list, rawStop) + 1

	// 处理范围
	if stop > list.Len() {
		stop = list.Len()
	}

	return start, stop
}

func parseMinus(list List.List, position int) int {
	if position < 0 {
		if list.Len()+position < 0 {
			return 0
		}

		return list.Len() + position
	}

	return position
}

func init() {
	engine.RegisterCommand("LPush", execLPush, writeFirstKey, -3, engine.FlagWrite)
	engine.RegisterCommand("LPushX", execLPushX, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("RPush", execRPush, writeFirstKey, -3, engine.FlagWrite)
	engine.RegisterCommand("RPushX", execRPushX, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("LPop", execLPop, writeFirstKey, 2, engine.FlagWrite)
	engine.RegisterCommand("RPop", execRPop, writeFirstKey, 2, engine.FlagWrite)
	engine.RegisterCommand("LIndex", execLIndex, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("LLen", execLLen, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("LRem", execLRem, writeFirstKey, 4, engine.FlagWrite)
	engine.RegisterCommand("LTrim", execLTrim, writeFirstKey, 4, engine.FlagWrite)
	engine.RegisterCommand("LRange", execLRange, readFirstKey, 4, engine.FlagReadOnly)
	engine.RegisterCommand("LSet", execLSet, writeFirstKey, 4, engine.FlagWrite)
}
