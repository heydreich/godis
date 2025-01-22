package connection

import (
	"godis/interface/redis"
	"godis/lib/logger"
	"godis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

type Connection struct {
	conn net.Conn

	sendingData wait.Wait

	mu sync.Mutex

	password   string
	selectedDB int

	isMulti        bool       //
	queue          [][][]byte //waiting command
	syntaxErrQueue []redis.Reply
	watching       map[string]uint32
	TxID           string // transaction ID

	subscribeChannels map[string]struct{}
}

var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

func (c *Connection) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}

	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()
	return c.conn.Write(bytes)
}

// Close disconnect with the client
func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.sendingData = wait.Wait{}
	c.password = ""
	c.selectedDB = 0
	c.isMulti = false
	c.queue = nil
	c.syntaxErrQueue = nil
	c.watching = nil
	c.TxID = ""
	c.subscribeChannels = nil
	connPool.Put(c)
	return nil
}

func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{conn: conn}
	}
	c.conn = conn
	return c
}
