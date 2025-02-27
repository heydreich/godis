package publish

import (
	"godis/interface/redis"
	"godis/lib/sync/wait"
	"sync"
	"time"
)

const (
	maxMessageInChan = 1024
)

var (
	MessageHeader     = "message"
	SubscribeHeader   = "subscribe"
	UnsubscribeHeader = "unsubscribe"
)

type channel struct {
	name          string                        // 管道名
	messageCh     chan []byte                   // 当前管道中待发送的消息
	subscriberNum int                           // 订阅者的数量
	subscribers   map[redis.Connection]struct{} // 这个管道所有的订阅者，在redis中用链表实现，而在 simple-redis 中使用 map(set)

	closed chan struct{}
	wait   wait.Wait
	mu     sync.Mutex
}

func newChannel(name string) *channel {
	c := &channel{
		name:        name,
		messageCh:   make(chan []byte, maxMessageInChan),
		subscribers: make(map[redis.Connection]struct{}),
		closed:      make(chan struct{}, 1),
	}
	return c
}

func (c *channel) publish(message []byte) int {
	c.mu.Lock()
	result := c.subscriberNum
	if result == 0 {
		return result
	}
	c.mu.Unlock()

	c.messageCh <- message

	return result
}

// 删除一个订阅者
func (c *channel) deleteSubscriber(client redis.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.subscribers, client)
	c.subscriberNum--

	client.CancelSubscribeChannel(c.name)
	code := client.GetSubscribeNum()

	_, _ = client.Write(makeMsg(UnsubscribeHeader, c.name, code))
}

func (c *channel) close() {
	c.closed <- struct{}{}
	c.wait.WaitWithTimeout(time.Second * 5)
}

func (c *channel) addSubscriber(client redis.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subscribers[client] = struct{}{}
	c.subscriberNum++

	client.AddSubscribeChannel(c.name)
	code := client.GetSubscribeNum()

	_, _ = client.Write(makeMsg(UnsubscribeHeader, c.name, code))
}

// 循环从c.messageCh消息管道中取出消息，然后进行发送
func (c *channel) loopSendMessage() {
	for {
		select {
		case <-c.closed:
			return
		case m := <-c.messageCh:
			c.doSend(m)
		}
	}
}

// 每一次发送消息的实际操作
func (c *channel) doSend(message []byte) {
	c.mu.Lock()
	// 记录订阅者
	subscribers := make([]redis.Connection, 0, c.subscriberNum)
	for subscriber := range c.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	c.mu.Unlock()

	// 发送订阅消息
	c.wait.Add(len(subscribers))
	for _, subscriber := range subscribers {
		_, _ = subscriber.Write(makeMsg(MessageHeader, c.name, message))
		c.wait.Done()
	}
}

func makeMsg(header string, name string, value interface{}) []byte {
	//todo
	var msg []byte

	return msg
}
