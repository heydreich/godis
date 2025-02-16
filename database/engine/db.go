package engine

import (
	"godis/datastruct/dict"
	"godis/datastruct/lock"
	"godis/interface/database"
	"godis/interface/redis"
	"godis/lib/utils"
	"godis/redis/protocol"
	"strings"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockSize     = 1024
)

type CmdLine = [][]byte

type DB struct {
	index      int                // 数据库号
	data       dict.Dict          //是一个 dict.Dict 接口类型的属性，记录数据库中所有的数据。
	ttlMap     dict.Dict          //用来记录所有 key 的过期时间。
	versionMap dict.Dict          //用来记录所有 key 的版本号，在事务中会用到。
	locker     *lock.Locks        //就是之前的 LockMap，用于一次性加锁，实现对数据的互斥访问。
	addAof     func(line CmdLine) //用于AOF持久化
}

func MakeDB() *DB {
	return &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		locker:     lock.Make(lockSize),
		addAof:     func(line CmdLine) {},
	}
}

func MakeBasicDB() *DB {
	return &DB{
		data:   dict.MakeSimpleDict(),
		ttlMap: dict.MakeSimpleDict(),
		locker: lock.Make(1),
		addAof: func(line CmdLine) {},
	}
}

func (db *DB) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
	if client.GetMultiStatus() {
		if errReply := db.CheckSyntaxErr(cmdLine); errReply != nil {
			client.EnqueueSyntaxErrQueue(errReply)
			return errReply
		}
		if errReply := db.CheckSupportMulti(cmdLine); errReply != nil {
			client.EnqueueSyntaxErrQueue(errReply) // 语法有错误
			return errReply
		}

		// 语法没有错误，则进入队列等待执行
		client.EnqueueCmdLine(cmdLine)

		return protocol.MakeStatusReply("QUEUED")
	}
	return db.execNormalCommand(cmdLine)
}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	if errReply := db.CheckSyntaxErr(cmdLine); errReply != nil {
		return errReply
	}

	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd := cmdTable[cmdName]

	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	db.locker.RWLocks(write, read)
	defer db.locker.RWUnlocks(write, read)

	funE := cmd.executor
	r, aofExpireCtx := funE(db, cmdLine[1:])
	db.afterExec(r, aofExpireCtx, cmdLine)
	if !IsReadOnlyCommand(cmdName) && !protocol.IsErrorReply(r) {
		db.AddVersion(write...)
	}

	return r
}

func (db *DB) CheckSyntaxErr(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 获取命令
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	return nil
}

func (db *DB) CheckSupportMulti(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd := cmdTable[cmdName]
	if cmd.prepare == nil {
		return protocol.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}

	return nil
}

func (db *DB) afterExec(r redis.Reply, aofExpireCtx *AofExpireCtx, cmdLine [][]byte) {
	key := string(cmdLine[1])

	if aofExpireCtx != nil && aofExpireCtx.NeedAof {
		db.addAof(cmdLine)
		if aofExpireCtx.ExpireAt != nil {
			db.addAof(utils.ExpireToCmdLine(key, *aofExpireCtx.ExpireAt))
		}
	}
}

// Flush Warning! clean all db data
func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
	db.locker = lock.Make(lockSize)
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func (db *DB) SetIndex(i int) {
	db.index = i
}

func (db *DB) GetIndex() int {
	return db.index
}

func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, val interface{}) bool {
		entity, _ := val.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}
		return cb(key, entity, expiration)
	})
}

func (db *DB) GetDBSize() (int, int) {
	return db.data.Len(), db.ttlMap.Len()
}

func (db *DB) SetAddAof(addAof func(line CmdLine)) {
	db.addAof = addAof
}

func (db *DB) ExecWithLock(cmdLine CmdLine) redis.Reply {
	if errReply := db.CheckSyntaxErr(cmdLine); errReply != nil {
		// 检查是否有语法错误
		return errReply
	}

	// 执行
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd := cmdTable[cmdName]
	fun := cmd.executor
	r, aofExpireCtx := fun(db, cmdLine[1:])
	db.afterExec(r, aofExpireCtx, cmdLine)

	return r
}
