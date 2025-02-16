package database

import (
	"godis/interface/redis"
	"time"
)

type CmdLine = [][]byte

type DB interface {
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}

type DBEngine interface {
	DB

	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)

	GetDBSize(dbIndex int) (int, int)
}

type DataEntity struct {
	Data interface{}
}
