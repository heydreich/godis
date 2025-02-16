package database

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"godis/config"
	"godis/database/aof"
	"godis/database/cluster"
	"godis/database/engine"
	"godis/database/publish"
	"godis/interface/database"
	"godis/interface/redis"
	"godis/lib/logger"
	"godis/lib/utils"
	"godis/redis/connection"
	"godis/redis/protocol"
)

type Server struct {
	dbSet        []*atomic.Value
	AofPersister *aof.Persister
	AofFileSize  int64
	rewriteWait  sync.WaitGroup
	rewriting    atomic.Bool
	closed       chan struct{}
	cluster      *cluster.Cluster
	publish      publish.Publish
}

func initServer() *Server {
	server := &Server{
		closed: make(chan struct{}, 1),
	}
	if config.Properties.Databases <= 0 {
		config.Properties.Databases = 16
	}
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range server.dbSet {
		singleDB := engine.MakeDB()
		singleDB.SetIndex(i)
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}

	if config.Properties.AppendOnly {
		if config.Properties.AofFilename == "" {
			config.Properties.AofFilename = "dump.aof"
		}
		server.AofFileSize = utils.GetFileSizeByName(config.Properties.AofFilename)

		AofPersister, err := aof.NewPersister(server, config.Properties.AofFilename, true, config.Properties.AofFsync, MakeAuxiliaryServer)
		if err != nil {
			logger.Fatal(err)
		}
		server.bindPersister(AofPersister)

		if config.Properties.AutoAofRewrite {
			if config.Properties.AutoAofRewritePercentage <= 0 {
				config.Properties.AutoAofRewritePercentage = 100
			}
			if config.Properties.AutoAofRewriteMinSize <= 0 {
				config.Properties.AutoAofRewriteMinSize = 16
			}

			go server.autoAofRewrite()
		}
	}

	return server
}

func NewStandaloneServer() *Server {
	server := initServer()
	return server
}

func NewClusterServer(peers []string) *Server {
	server := initServer()

	cluster := cluster.NewCluster(config.Properties.Self)
	cluster.AddPeers(peers...)
	if cluster == nil {
		logger.Fatalf("please set 'self'(self ip:port) in conf file")
	}
	server.cluster = cluster
	return server
}

func (s *Server) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
	if s.cluster == nil {
		return s.execCluster(client, cmdLine)
	}

	return s.execStandalone(client, cmdLine)
}

func (s *Server) execStandalone(client redis.Connection, cmdLine [][]byte) redis.Reply {

	cmdName := strings.ToLower(string(cmdLine[0]))

	if cmdName == "ping" {
		logger.Debugf("received heart beat from %v", client.Name())
		return protocol.MakePongReply()
	}

	if _, ok := client.(*connection.FakeConn); !ok {
		if cmdName == "auth" {
			return Auth(client, cmdLine[1:])
		}
		if !isAuthenticated(client) {
			return protocol.MakeErrReply("NOAUTH Authentication required")
		}
	}

	switch cmdName {
	case "select":
		return SelectDB(client, cmdLine[1:], len(s.dbSet))
	case "bgrewriteaof":
		return BGRewriteAof(s, cmdLine[1:])
	case "rewriteaof":
		return RewriteAof(s, cmdLine[1:])
	case "multi":
		return StartMultiStandalone(client, cmdLine[1:])
	case "exec":
		return ExecMultiStandalone(s, client, cmdLine[1:])
	case "discard":
		return DiscardMultiStandalone(client, cmdLine[1:])
	case "watch":
		return ExecWatchStandalone(s, client, cmdLine[1:])
	case "unwatch":
		return ExecUnWatchStandalone(client, cmdLine[1:])
	case "publish":
		return Publish(s, cmdLine[1:])
	case "subscribe":
		return Subscribe(s, client, cmdLine[1:])
	case "unsubscribe":
		return UnSubscribe(s, client, cmdLine[1:])
	case "pubsub":
		return PubSub(s, cmdLine[1:])
	}

	dbIndex := client.GetDBIndex()
	selectedDB, errReply := s.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(client, cmdLine)
}
func (s *Server) execCluster(client redis.Connection, cmdLine [][]byte) redis.Reply {
	return nil
	// cmdName := strings.ToLower(string(cmdLine[0]))

	// if (cmdName) == "ping" {
	// 	logger.Debugf("received heart beat from %v", client.Name())
	// 	return protocol.MakePongReply()
	// }

	// if _, ok := client.(*connection.FakeConn); !ok { // fakeConn不做校验
	// 	if cmdName == "auth" {
	// 		return Auth(client, cmdLine[1:])
	// 	}
	// 	if !isAuthenticated(client) {
	// 		return protocol.MakeErrReply("NOAUTH Authentication required")
	// 	}
	// }

	// switch cmdName {
	// case "select":
	// 	return SelectDB(client, cmdLine[1:], len(s.dbSet))
	// case "bgrewriteaof":
	// 	return BGRewriteAof(s, cmdLine[1:])
	// case "rewriteaof":
	// 	return RewriteAof(s, cmdLine[1:])
	// case "multi":
	// 	return s.cluster.StartMultiCluster(client, cmdLine[1:])
	// case "exec":
	// 	return s.cluster.ExecMultiCluster(client, cmdLine[1:])
	// case "discard":
	// 	return s.cluster.DiscardMultiCluster(client, cmdLine[1:])
	// case "try":
	// 	dbIndex := client.GetDBIndex()
	// 	localDB, errReply := s.selectDB(dbIndex)
	// 	if errReply != nil {
	// 		return errReply
	// 	}
	// 	return s.cluster.Try(localDB, cmdLine[1:])
	// case "commit":
	// 	return s.cluster.Commit(cmdLine[1:])
	// case "cancel":
	// 	return s.cluster.Cancel(cmdLine[1:])
	// case "end":
	// 	return s.cluster.End(cmdLine[1:])
	// case "watch":
	// 	dbIndex := client.GetDBIndex()
	// 	localDB, errReply := s.selectDB(dbIndex)
	// 	if errReply != nil {
	// 		return errReply
	// 	}
	// 	return s.cluster.Watch(dbIndex, localDB, client, cmdLine[1:])
	// case "unwatch":
	// 	return s.cluster.UnWatch(client, cmdLine[1:])
	// case "subscribe":
	// 	return Subscribe(s, client, cmdLine[1:])
	// case "unsubscribe":
	// 	return UnSubscribe(s, client, cmdLine[1:])
	// case "singlepublish":
	// 	return Publish(s, cmdLine[1:])
	// case "singlepubsub":
	// 	return PubSub(s, cmdLine[1:])
	// case "publish":
	// 	return PublishCluster(s, cmdLine[1:])
	// case "pubsub":
	// 	return PubSubCluster(s, cmdLine[1:])
	// }

	// // normal commands
	// dbIndex := client.GetDBIndex()
	// localDB, errReply := s.selectDB(dbIndex)
	// if errReply != nil {
	// 	return errReply
	// }

	// return s.cluster.Exec(client, dbIndex, localDB, cmdLine)
}

func (s *Server) AfterClientClose(c redis.Connection) {
	UnSubscribe(s, c, nil)
}

func (s *Server) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db := s.mustSelectDB(dbIndex)
	db.ForEach(cb)
}

func (s *Server) GetDBSize(dbIndex int) (int, int) {
	db := s.mustSelectDB(dbIndex)
	return db.GetDBSize()
}

func (s *Server) mustSelectDB(dbIndex int) *engine.DB {
	selectDB, err := s.selectDB(dbIndex)
	if err != nil {
		panic(err)
	}
	return selectDB
}

func (s *Server) Close() {
	s.closed <- struct{}{}
	if config.Properties.AppendOnly {
		s.AofPersister.Close()
	}

	if s.cluster != nil {
		s.cluster.Close()
	}

	s.publish.Close()
}

func (s *Server) selectDB(dbIndex int) (*engine.DB, *protocol.StandardErrReply) {
	if dbIndex >= len(s.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR DB index is out of range")
	}
	return s.dbSet[dbIndex].Load().(*engine.DB), nil
}

func (s *Server) autoAofRewrite() {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ticker.C:
			if s.rewriting.Load() {
				continue
			}
			s.rewriteWait.Add(1)
			aofFileSize := utils.GetFileSizeByName(config.Properties.AofFilename)

			if aofFileSize > s.AofFileSize*config.Properties.AutoAofRewritePercentage/100 &&
				aofFileSize > config.Properties.AutoAofRewriteMinSize*1024*1024 {
				go s.AofPersister.Rewrite(&s.rewriteWait, &s.rewriting)
				s.rewriteWait.Wait()
				s.AofFileSize = aofFileSize
			} else {
				s.rewriteWait.Done()
			}
		case <-s.closed:
			ticker.Stop()
			return
		}
	}
}
