package aof

import (
	"context"
	"errors"
	"godis/interface/database"
	"godis/lib/logger"
	"godis/lib/utils"
	"godis/redis/connection"
	"godis/redis/parser"
	"godis/redis/protocol"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	FsyncAlways = iota
	FsyncEverySec
	FsyncNo
)

type CmdLine [][]byte

const (
	aofQueueSize = 1 << 16
)

type Persister struct {
	ctx         context.Context
	cancel      context.CancelFunc
	db          database.DBEngine
	tmpDBMaker  func() database.DBEngine
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	aofFsync    int // AOF 刷盘策略
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	// 表示正在aof重写，同时只有一个aof重写
	aofRewriting sync.WaitGroup
	currentDB    int
}

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

func NewPersister(db database.DBEngine, filename string, load bool, fsync int, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	if fsync < FsyncAlways || fsync > FsyncNo {
		return nil, errors.New("load aof failed, aof fsync must be: 0: always, 1: every sec, 2: no")
	}
	persister := &Persister{}
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.aofFilename = filename
	persister.aofFsync = fsync
	persister.currentDB = 0

	if load {
		persister.LoadAof(0)
	}

	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})

	go func() {
		persister.listenCmd()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}

	return persister, nil
}

func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.pausingAof.Lock()
				if err := persister.aofFile.Sync(); err != nil {
					logger.Errorf("fsync failed: %v", err)
				}
				persister.pausingAof.Unlock()
			case <-persister.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (persister *Persister) Close() {
	persister.aofRewriting.Wait()

	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished
		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	persister.cancel()
}

func (persister *Persister) LoadAof(maxBytes int64) {
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	// 打开 AOF 文件，从 AOF 文件中读取 maxBytes 字节的数据。
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}

		//执行
		r, ok := p.Data.(*protocol.MultiBulkReply)
		fakeConn := connection.NewFakeConn()
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		ret := persister.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}

		if strings.ToLower(string(r.Args[0])) == "select" {
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}

}

// 监听aofChan，写入 AOF 文件
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

func (persister *Persister) writeAof(p *payload) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// 首先，**选择正确的数据库**。
	// 每个客户端都可以选择自己的数据库，所以 payload 中要保存客户端选择的数据库。
	// **选择的数据库与 AOF 文件中当前的数据库不一致时写入一条 Select 命令**。
	if p.dbIndex != persister.currentDB {
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return
		}
		persister.currentDB = p.dbIndex
	}

	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}

	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	if persister.aofChan == nil {
		return
	}

	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}

	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
}
