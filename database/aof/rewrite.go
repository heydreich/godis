package aof

import (
	"godis/config"
	"godis/interface/database"
	"godis/lib/logger"
	"godis/lib/utils"
	"godis/redis/protocol"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type RewriteCtx struct {
	tmpFile  *os.File // 重写时用到的临时文件
	fileSize int64    // 重写时文件大小
	dbIndex  int      // 重写时的当前数据库
}

func (persister *Persister) newRewritePersister() *Persister {
	tmpDB := persister.tmpDBMaker()
	return &Persister{
		db:          tmpDB,
		aofFilename: persister.aofFilename,
	}
}

func (persister *Persister) Rewrite(rewriteWait *sync.WaitGroup, rewriting *atomic.Bool) error {
	logger.Info("rewrite aof start")
	persister.aofRewriting.Add(1)
	rewriting.Store(true)
	defer persister.aofRewriting.Done()
	defer func() {
		if rewriteWait != nil {
			rewriteWait.Done()
		}
		rewriting.Store(false)
		logger.Info("rewrite finished...")
	}()

	rewriteCtx, err := persister.StartRewrite()
	if err != nil {
		return err
	}

	err = persister.DoRewrite(rewriteCtx)
	if err != nil {
		return err
	}
	err = persister.FinishRewrite(rewriteCtx)
	if err != nil {
		return err
	}

	return nil
}

// StartRewrite 暂停 AOF 写入 ->  准备重写 -> 恢复AOF写入。
func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// 首先暂停aof写入
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	fileStat, _ := os.Stat(persister.aofFilename)
	fileSize := fileStat.Size()
	tmpFile, err := ioutil.TempFile("./", "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  tmpFile,
		fileSize: fileSize,
		dbIndex:  persister.currentDB,
	}, nil
}

// DoRewrite 用于重写协程读取 AOF 文件中的前一部分（重写开始前的数据，不包括读写过程中写入的数据）并重写到临时文件中。流程如下：
func (persister *Persister) DoRewrite(rewriteCtx *RewriteCtx) error {
	tmpFile := rewriteCtx.tmpFile

	rewritePersister := persister.newRewritePersister()
	rewritePersister.LoadAof(rewriteCtx.fileSize)

	for i := 0; i < config.Properties.Databases; i++ {
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		if _, err := tmpFile.Write(data); err != nil {
			return err
		}

		rewritePersister.db.ForEach(i, func(key string, data *database.DataEntity, expiration *time.Time) bool {
			bytes := utils.EntityToBytes(key, data)
			if bytes != nil {
				_, _ = tmpFile.Write(bytes)
			}
			if expiration != nil {
				bytes := utils.ExpireToBytes(key, *expiration)
				if bytes != nil {
					_, _ = tmpFile.Write(bytes)
				}
			}
			return true
		})
	}
	return nil
}

// FinishRewrite 暂停 AOF 写入 -> 将重写过程中产生的**新数据写入临时文件**中 -> 使用临时文件覆盖 AOF 文件（使用文件系统的 mv 命令保证安全） -> 恢复 AOF 写入。
func (persister *Persister) FinishRewrite(rewriteCtx *RewriteCtx) error {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	src, err := os.Open(persister.aofFilename)
	if err != nil {
		logger.Error("open aofFilename failed: " + err.Error())
		return err
	}

	_, err = src.Seek(rewriteCtx.fileSize, 0)
	if err != nil {
		logger.Error("seek failed: " + err.Error())
		return err
	}

	tmpFile := rewriteCtx.tmpFile

	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(rewriteCtx.dbIndex))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return err
	}

	_, err = io.Copy(tmpFile, src)
	if err != nil {
		logger.Error("copy aof file failed: " + err.Error())
		return err
	}

	_ = persister.aofFile.Close()
	_ = src.Close()
	_ = tmpFile.Close()
	_ = os.Rename(tmpFile.Name(), persister.aofFilename)

	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	persister.aofFile = aofFile

	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(rewriteCtx.dbIndex))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}

	return nil
}
