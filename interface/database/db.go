package database

import "godis/interface/redis"

type CmdLine = [][]byte

type DB interface {
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
	afterClientClose(c redis.Connection)
	Close()
}
