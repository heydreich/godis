package commands

import (
	"godis/database/engine"
	"godis/interface/redis"
	"godis/redis/protocol"
	"strconv"
	"time"
)

func init() {
	engine.RegisterCommand("Del", execDel, writeFirstKey, 2, engine.FlagWrite)
	engine.RegisterCommand("ExpireAt", execExpireAt, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("Expire", execExpire, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("KeyVersion", execKeyVersion, writeFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("Exist", execExist, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("Persist", execPersist, writeFirstKey, 2, engine.FlagWrite)
}

func execDel(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	_, exist := db.GetEntity(key)

	if !exist {
		// 不存在，直接返回
		return protocol.MakeIntReply(0), &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	db.Remove(key)
	return protocol.MakeIntReply(1), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execExpireAt(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	expireAt := time.Unix(raw, 0)

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0), nil
	}

	db.Expire(key, expireAt)
	return protocol.MakeIntReply(1), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: &expireAt,
	}
}

func execExpire(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	expireAt := time.Now().Add(time.Second * time.Duration(raw))

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0), nil
	}

	db.Expire(key, expireAt)
	return protocol.MakeIntReply(1), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: &expireAt,
	}
}

func execKeyVersion(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	version := db.GetVersion(key)

	return protocol.MakeIntReply(int64(version)), &engine.AofExpireCtx{
		NeedAof:  false,
		ExpireAt: nil,
	}
}

func execExist(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	_, exist := db.GetEntity(key)
	if !exist {
		return protocol.MakeIntReply(0), nil
	}

	return protocol.MakeIntReply(1), nil
}

func execPersist(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0), nil
	}

	_, exists = db.TTLMap().Get(key)
	if !exists {
		return protocol.MakeIntReply(0), nil
	}

	db.Persist(key)

	return protocol.MakeIntReply(1), &engine.AofExpireCtx{NeedAof: true}
}
