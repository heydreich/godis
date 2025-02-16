package engine

import (
	"godis/config"
	"godis/interface/redis"
	"godis/lib/utils"
	"godis/redis/protocol"
	"strings"
	"time"
)

// ExecMulti multi命令执行阶段
func (db *DB) ExecMulti(c redis.Connection) redis.Reply {
	return db.ExecMultiCommand(c.GetEnqueuedCmdLine(), c.GetWatching())
}

func (db *DB) ExecMultiCommand(cmdLines [][][]byte, watching map[string]uint32) redis.Reply {
	// 此时不需要检查是否有语法错误，因为在排队过程中已经检查过了

	// // 获取所有需要加锁的key
	writeKeys := make([]string, len(cmdLines))
	readKeys := make([]string, len(cmdLines)+len(watching))
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]

		prepare := cmd.prepare
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}

	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	readKeys = append(readKeys, watchingKeys...)
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)

	versionChanged := db.checkVersionChanged(watching)
	if versionChanged {
		return protocol.MakeNullBulkReply()
	}

	var results [][]byte
	var undoLogs [][]CmdLine
	aborted := false

	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]
		if config.Properties.OpenAtomicTx {
			key := string(cmdLine[1])
			undoLogs = append(undoLogs, db.GetUndoLog(key))
		}

		fn := cmd.executor
		r, aofExpireCtx := fn(db, cmdLine[1:])
		if config.Properties.OpenAtomicTx && protocol.IsErrorReply(r) {
			undoLogs = undoLogs[:len(undoLogs)-1]
			aborted = true
			break
		}
		results = append(results, []byte(r.DataString()))
		db.afterExec(r, aofExpireCtx, cmdLine)
	}

	if len(results) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	if config.Properties.OpenAtomicTx && aborted {
		size := len(undoLogs)
		for i := size - 1; i >= 0; i-- {
			undoLog := undoLogs[i]
			if len(undoLog) == 0 {
				continue
			}
			for _, cmdLine := range undoLog {
				db.ExecWithLock(cmdLine)
			}
		}
		return protocol.MakeErrReply("EXECABORT Transaction rollback because of errors during executing. (atomic tx is open)")
	}

	// 未开启原子性事务，或者执行成功
	// 写命令增加版本
	db.AddVersion(writeKeys...)
	return protocol.MakeMultiBulkReply(results)
}

func (db *DB) GetUndoLog(key string) []CmdLine {
	undoLog := make([]CmdLine, 0, 3)
	entity, exist := db.GetEntity(key)
	if !exist {
		// 不存在，直接删除key
		undoLog = append(undoLog, utils.ToCmdLine("DEL", key))
	} else {
		// 存在，首先删除新的值
		undoLog = append(undoLog, utils.ToCmdLine("DEL", key))
		// 接着恢复为原来的值
		undoLog = append(undoLog, utils.EntityToCmdLine(key, entity))
		// 设置 TTL
		if raw, ok := db.ttlMap.Get(key); ok { // 获取过期时间
			// 如果有过期时间
			expireTime, _ := raw.(time.Time)
			// 设置过期时间
			undoLog = append(undoLog, utils.ExpireToCmdLine(key, expireTime))
		}
	}

	return undoLog
}
