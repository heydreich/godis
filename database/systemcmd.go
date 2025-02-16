package database

import (
	"godis/config"
	"godis/interface/redis"
	"godis/redis/protocol"
	"strconv"
)

func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	if config.Properties.Password == "" {
		return protocol.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	passwd := string(args[0])
	c.SetPassword(passwd)
	if config.Properties.Password != passwd {
		return protocol.MakeErrReply("ERR invalid password")
	}
	return protocol.MakeOkReply()
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.Password == "" {
		return true
	}
	return c.GetPassword() == config.Properties.Password
}

func SelectDB(c redis.Connection, args [][]byte, dbNum int) redis.Reply {
	if c.GetMultiStatus() {
		errReply := protocol.MakeErrReply("cannot select database within multi")
		c.EnqueueSyntaxErrQueue(errReply)
		return errReply
	}

	if len(c.GetWatching()) > 0 {
		return protocol.MakeErrReply("cannot select database when watching")
	}

	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'select' command")
	}

	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR select db index is not an integer")
	}

	if dbIndex < 0 || dbIndex >= dbNum {
		return protocol.MakeErrReply("ERR index is invalid")
	}

	c.SelectDB(dbIndex)

	return protocol.MakeOkReply()
}

func BGRewriteAof(s *Server, args [][]byte) redis.Reply {
	if s.rewriting.Load() {
		return protocol.MakeStatusReply("Background append only file rewriting doing")
	}

	s.rewriteWait.Add(1)
	go s.AofPersister.Rewrite(&s.rewriteWait, &s.rewriting)
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

func RewriteAof(s *Server, args [][]byte) redis.Reply {
	if s.rewriting.Load() {
		s.rewriteWait.Wait()
		return protocol.MakeOkReply()
	}

	s.rewriteWait.Add(1)
	err := s.AofPersister.Rewrite(&s.rewriteWait, &s.rewriting)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}
