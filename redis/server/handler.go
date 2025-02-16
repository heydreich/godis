package server

import (
	"context"
	"godis/config"
	database2 "godis/database"
	"godis/interface/database"
	"godis/lib/logger"
	"godis/lib/sync/atomic"
	"godis/redis/connection"
	"godis/redis/parser"
	"godis/redis/protocol"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

var unknownErrReplyBytes = []byte("-ERR unknown\r\n")

type Handler struct {
	activeConn  sync.Map // value记录activeConn的心跳
	db          database.DB
	closing     atomic.Boolean // refusing new client and new request
	closingChan chan struct{}  // 停止心跳检查计时器
}

func MakeHandler() *Handler {
	var db database.DB
	if config.Properties.Peers != nil && len(config.Properties.Peers) != 0 {
		db = database2.NewClusterServer(config.Properties.Peers)
		logger.Infof("cluster mode, peer is %v", config.Properties.Peers)
	} else {
		db = database2.NewStandaloneServer()
	}
	h := &Handler{
		db:          db,
		closingChan: make(chan struct{}, 1),
	}

	if config.Properties.Keepalive > 0 {
		go h.checkActiveHeartbeat(config.Properties.Keepalive)
	}
	return h
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		conn.Close()
		return
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, time.Now())

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}

		h.activeConn.Store(client, time.Now())

		result := h.db.Exec(client, r.Args)

		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}
	}
}

func (h *Handler) checkActiveHeartbeat(keepalive int) {
	ticker := time.NewTicker(time.Second * time.Duration(keepalive/2))
	for {
		select {
		case <-ticker.C:
			h.activeConn.Range(func(key, value any) bool {
				if time.Now().After(value.(time.Time).Add(time.Second * time.Duration(keepalive))) {
					h.closeClient(key.(*connection.Connection))
				}
				return true
			})
		case <-h.closingChan:
			ticker.Stop()
			return
		}
	}
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	h.closingChan <- struct{}{}
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool { // close all active conn
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
