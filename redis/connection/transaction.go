package connection

import "godis/interface/redis"

func (c *Connection) GetMultiStatus() bool {
	return c.isMulti
}
func (c *Connection) SetMultiStatus(isMulti bool) {
	c.isMulti = isMulti
}
func (c *Connection) GetEnqueuedCmdLine() [][][]byte {
	return c.queue
}
func (c *Connection) EnqueueCmdLine(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}
func (c *Connection) ClearCmdLines() {
	c.queue = nil
}
func (c *Connection) GetSyntaxErrQueue() []redis.Reply {
	return c.syntaxErrQueue
}
func (c *Connection) EnqueueSyntaxErrQueue(r redis.Reply) {
	c.syntaxErrQueue = append(c.syntaxErrQueue, r)
}
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}
func (c *Connection) CancelWatching() {
	c.watching = nil
}
func (c *Connection) SetTxID(id string) {
	c.TxID = id
}
func (c *Connection) GetTxID() string {
	return c.TxID
}
