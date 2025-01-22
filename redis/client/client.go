package client

import (
	"godis/interface/redis"
	"godis/lib/logger"
	"godis/lib/sync/wait"
	"godis/redis/parser"
	"godis/redis/protocol"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	running
	closed
)
const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

type Client struct {
	conn        net.Conn      // 与服务器的tcp连接
	pendingReqs chan *request // 等待发送的请求
	waitingReqs chan *request // 等待服务器响应的请求
	ticker      *time.Ticker  // 发送心跳的计时器
	addr        string

	isCmdLine  bool
	curDBIndex int

	status  int32
	working *sync.WaitGroup

	keepalive time.Duration
}

type request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait // 调用协程发送请求后通过 waitgroup 等待请求异步处理完成
	err       error
}

func (client *Client) Start() {
	go client.handleWrite()
	go client.handleRead()

	if client.keepalive > 0 {
		client.ticker = time.NewTicker(time.Second * client.keepalive / 2)
		go client.heartbeat()
	}
	atomic.StoreInt32(&client.status, running)
}

func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	if client.keepalive > 0 {
		client.ticker.Stop()
	}
	close(client.pendingReqs)
	client.working.Wait()
	_ = client.conn.Close()
	close(client.waitingReqs)
}

func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrReply("client closed")
	}

	request := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()

	client.pendingReqs <- request
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	if request.err != nil {
		return protocol.MakeErrReply("request failed")
	}

	if _, ok := request.reply.(*protocol.StandardErrReply); !ok && strings.ToLower(string(args[0])) == "select" {
		curDBIndex, _ := strconv.Atoi(string(args[1]))
		client.curDBIndex = curDBIndex
	}
	return request.reply
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

func (client *Client) doRequest(req *request) {
	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()

	var err error
	for i := 0; i < 3; i++ {
		_, err = client.conn.Write(bytes)
		if err == nil || (!strings.Contains(err.Error(), "timeout") &&
			!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	if err == nil {
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) handleRead() {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			// client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	request := <-client.waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) heartbeat() {
	req := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- req
	req.waiting.WaitWithTimeout(maxWait)
}
