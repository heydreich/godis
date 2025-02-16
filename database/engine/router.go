package engine

import (
	"godis/interface/redis"
	"strings"
	"time"
)

// AofExpireCtx 记录在执行命令时，是否需要AOF持久化，是否有过期时间
type AofExpireCtx struct {
	NeedAof  bool
	ExpireAt *time.Time
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) (redis.Reply, *AofExpireCtx)

// PreFunc returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

var cmdTable = make(map[string]*command)

type command struct {
	executor ExecFunc //在命令真正执行时会调用这个函数。
	prepare  PreFunc  // return related keys command,用于解析出命令中需要加读锁和写锁的 keys
	arity    int      // allow number of args, arity < 0 means len(args) >= -arity,合法的参数数量，
	flags    int      // 记录这个命令是只读命令还是涉及到了写操作,flagWrite or flagReadOnly
}

const (
	FlagWrite    = 0
	FlagReadOnly = 1
)

func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, arity int, flags int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		prepare:  prepare,
		arity:    arity,
		flags:    flags,
	}
}

func IsReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	if cmd, ok := cmdTable[name]; ok && (cmd.flags&FlagReadOnly > 0) {
		return true
	}

	return false
}
