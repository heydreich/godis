package commands

import (
	"godis/database/engine"
	Dict "godis/datastruct/dict"
	"godis/interface/database"
	"godis/interface/redis"
	"godis/redis/protocol"
	"strconv"
)

func init() {
	engine.RegisterCommand("Hset", execHSet, writeFirstKey, -4, engine.FlagWrite)
	engine.RegisterCommand("HSetNX", execHSetNX, writeFirstKey, 4, engine.FlagWrite)
	engine.RegisterCommand("HGet", execHGet, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("HExists", execHExists, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("HGetAll", execHGetAll, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("HIncrBy", execHIncrBy, writeFirstKey, 4, engine.FlagWrite)
	engine.RegisterCommand("HKeys", execHKeys, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("HVals", execHVals, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("HLen", execHLen, readFirstKey, 2, engine.FlagReadOnly)
}

func execHSet(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args)%2 != 1 {
		return protocol.MakeArgNumErrReply("hset"), nil
	}

	key := string(args[0])

	dict, _, errReply := getOrInitDict(db, key)
	if errReply != nil {
		return errReply, nil
	}

	result := 0
	for i := 1; i < len(args); i += 2 {
		field := string(args[i])
		value := args[i+1]
		result += dict.Put(field, value)
	}

	return protocol.MakeIntReply(int64(result)), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func getOrInitDict(db *engine.DB, key string) (dict Dict.Dict, inited bool, errReply protocol.ErrorReply) {
	dict, errReply = getAsDict(db, key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if dict == nil {
		dict = Dict.MakeSimpleDict()
		db.PutEntity(key, &database.DataEntity{
			Data: dict,
		})
		inited = true
	}
	return dict, inited, nil
}

func getAsDict(db *engine.DB, key string) (dict Dict.Dict, errorReply protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	dict, ok := entity.Data.(Dict.Dict)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return dict, nil
}

func execHSetNX(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	dict, _, errReply := getOrInitDict(db, key)
	if errReply != nil {
		return errReply, nil
	}

	result := dict.PutIfAbsent(field, value)
	if result == 0 {
		return protocol.MakeIntReply(int64(result)), nil
	}

	return protocol.MakeIntReply(int64(result)), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execHGet(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	field := string(args[1])

	// get entity
	dict, errReply := getAsDict(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if dict == nil {
		return protocol.MakeNullBulkReply(), nil
	}

	raw, exists := dict.Get(field)
	if !exists {
		return protocol.MakeNullBulkReply(), nil
	}
	value, _ := raw.([]byte)
	return protocol.MakeBulkReply(value), nil
}

func execHExists(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	field := string(args[1])

	// get entity
	dict, errReply := getAsDict(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if dict == nil {
		return protocol.MakeIntReply(0), nil
	}

	_, exists := dict.Get(field)
	if !exists {
		return protocol.MakeIntReply(0), nil
	}

	return protocol.MakeIntReply(1), nil
}

func execHGetAll(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	// get entity
	dict, errReply := getAsDict(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if dict == nil {
		return protocol.MakeEmptyMultiBulkReply(), nil
	}

	var results [][]byte
	dict.ForEach(func(key string, val interface{}) bool {
		// 记录所有的key和value
		results = append(results, []byte(key))
		results = append(results, val.([]byte))

		return true
	})

	return protocol.MakeMultiBulkReply(results), nil
}

func execHIncrBy(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	field := string(args[1])
	by, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
	}

	dict, _, errReply := getOrInitDict(db, key)
	if errReply != nil {
		return errReply, nil
	}

	var valueInt int64
	raw, exist := dict.Get(field)
	if exist {
		value, _ := raw.([]byte)
		valueInt, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
		}
		if (valueInt > 0 && by > 0 && valueInt+by <= 0) || (valueInt < 0 && by < 0 && valueInt+by >= 0) {
			// 判断溢出
			return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
		}
	}

	dict.Put(field, []byte(strconv.FormatInt(valueInt+by, 10)))
	return protocol.MakeIntReply(valueInt + by), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execHKeys(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	// get entity
	dict, errReply := getAsDict(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if dict == nil {
		return protocol.MakeEmptyMultiBulkReply(), nil
	}

	var results [][]byte
	dict.ForEach(func(key string, _ interface{}) bool {
		results = append(results, []byte(key)) //记录所有的key

		return true
	})

	return protocol.MakeMultiBulkReply(results), nil
}

func execHVals(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	// get entity
	dict, errReply := getAsDict(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if dict == nil {
		return protocol.MakeEmptyMultiBulkReply(), nil
	}

	var results [][]byte
	dict.ForEach(func(_ string, val interface{}) bool {
		results = append(results, val.([]byte)) //记录所有的key

		return true
	})

	return protocol.MakeMultiBulkReply(results), nil
}

func execHLen(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	dict, errReply := getAsDict(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if dict == nil {
		return protocol.MakeIntReply(0), nil
	}

	length := dict.Len()
	return protocol.MakeIntReply(int64(length)), nil
}
