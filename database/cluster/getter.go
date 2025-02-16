package cluster

import (
	"godis/config"
	"godis/interface/redis"
	"godis/lib/pool"
	"godis/lib/utils"
	"godis/redis/client"
	"godis/redis/protocol"
	"strconv"
)

type getter struct {
	addr    string
	poolMap map[int]*pool.Pool
}

func newGetter(addr string) *getter {
	finalizer := func(x interface{}) {
		if c, ok := x.(*client.Client); ok {
			c.Close()
		}
	}

	checkAlive := func(x interface{}) bool {
		if c, ok := x.(*client.Client); ok {
			return c.StatusClosed()
		}

		return false
	}

	poolMap := make(map[int]*pool.Pool, config.Properties.Databases)
	for i := 0; i < config.Properties.Databases; i++ {
		var dbIndex = i
		factory := func() (interface{}, error) {
			c, err := client.MakeClient(addr, config.Properties.Keepalive)
			if err != nil {
				return nil, err
			}

			c.Start()
			if config.Properties.Password != "" {
				r := c.Send(utils.ToCmdLine("AUTH", config.Properties.Password))
				if protocol.IsErrorReply(r) {
					c.Close()
					return nil, protocol.MakeErrReply("ERR cluster password is required, please set same password in cluster")
				}
			}
			r := c.Send(utils.ToCmdLine("SELECT", strconv.Itoa(dbIndex)))
			if protocol.IsErrorReply(r) {
				c.Close()
				return nil, protocol.MakeErrReply("ERR cluster password is required, please set same password in cluster")
			}
			return c, nil
		}
		poolMap[dbIndex] = pool.New(factory, finalizer, checkAlive, pool.Config{
			MaxIdleNum:   8,
			MaxActiveNum: 16,
			MaxRetryNum:  1,
		})
	}
	return &getter{
		addr:    addr,
		poolMap: poolMap,
	}
}

func (g *getter) RemoteExec(dbIndex int, args [][]byte) redis.Reply {
	raw, err := g.poolMap[dbIndex].Get()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer g.poolMap[dbIndex].Put(raw)
	c, _ := raw.(*client.Client)
	return c.Send(args)
}

func (g *getter) Close() {
	for _, p := range g.poolMap {
		p.Close()
	}
}
