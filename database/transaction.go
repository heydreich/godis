package database

import (
	"godis/interface/redis"
	"godis/redis/protocol"
)

func StartMultiStandalone(client redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeArgNumErrReply("multi")
	}

	if client.GetMultiStatus() {
		return protocol.MakeErrReply("ERR MULTI calls can not be nested")
	}

	client.SetMultiStatus(true)
	return protocol.MakeOkReply()
}

func ExecMultiStandalone(s *Server, client redis.Connection, args [][]byte) redis.Reply {
	if !client.GetMultiStatus() {
		return protocol.MakeErrReply("ERR EXEC without MULTI")
	}
	if len(args) != 0 {
		return protocol.MakeArgNumErrReply("exec")
	}
	defer client.SetMultiStatus(false)
	defer client.CancelWatching()

	if len(client.GetSyntaxErrQueue()) > 0 {
		return protocol.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
	}

	dbIndex := client.GetDBIndex()
	localDB, errReply := s.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return localDB.ExecMulti(client)
}

func DiscardMultiStandalone(client redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeArgNumErrReply("exec")
	}

	if !client.GetMultiStatus() {
		return protocol.MakeErrReply("ERR DISCARD without MULTI")
	}

	defer client.SetMultiStatus(false)
	defer client.CancelWatching()

	return protocol.MakeOkReply()
}

func ExecWatchStandalone(s *Server, client redis.Connection, args [][]byte) redis.Reply {
	if client.GetMultiStatus() {
		return protocol.MakeErrReply("ERR WATCH inside MULTI is not allowed")
	}

	if len(args) < 0 {
		return protocol.MakeArgNumErrReply("watch")
	}

	dbIndex := client.GetDBIndex()
	localDB, errReply := s.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}

	watching := client.GetWatching()
	for _, rawKey := range args {
		key := string(rawKey)
		watching[key] = localDB.GetVersion(key)
	}

	return protocol.MakeOkReply()
}

func ExecUnWatchStandalone(client redis.Connection, args [][]byte) redis.Reply {
	if client.GetMultiStatus() {
		return protocol.MakeErrReply("ERR UNWATCH inside MULTI is not allowed")
	}

	if len(args) != 0 { // 参数数量不正确
		return protocol.MakeArgNumErrReply("unwatch")
	}

	// 取消watch
	client.CancelWatching()

	return protocol.MakeOkReply()
}
