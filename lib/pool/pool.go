package pool

import (
	"errors"
	"sync"
)

type (
	FactoryFunc    func() (interface{}, error)
	FinalizerFunc  func(x interface{})
	CheckAliveFunc func(x interface{}) bool
)

var (
	ErrClosed    = errors.New("pool closed")
	ErrMaxActive = errors.New("active connections reached max num")
)

type Config struct {
	MaxIdleNum   int // 最大空闲连接数
	MaxActiveNum int // 最大活跃连接数
	MaxRetryNum  int
}

type Pool struct {
	Config

	factory       FactoryFunc      // 创建连接
	finalizer     FinalizerFunc    // 关闭连接
	checkAlive    CheckAliveFunc   // 检查连接是否存活
	idles         chan interface{} // 空闲的连接
	activeConnNum int              // 活跃连接数
	closed        bool

	mu sync.Mutex
}

func New(factory FactoryFunc, finalizer FinalizerFunc, checkAlive CheckAliveFunc, cfg Config) *Pool {
	return &Pool{
		Config:        cfg,
		factory:       factory,
		finalizer:     finalizer,
		checkAlive:    checkAlive,
		activeConnNum: 0,
		idles:         make(chan interface{}, cfg.MaxIdleNum),
		closed:        false,
	}
}

func (pool *Pool) Get() (interface{}, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.closed {
		return nil, ErrClosed
	}

	select {
	case item := <-pool.idles:
		if !pool.checkAlive(item) {
			var err error
			item, err = pool.getItem()
			if err != nil {
				return nil, err
			}
		}
		pool.activeConnNum++
		return item, nil
	default:
		item, err := pool.getItem()
		if err != nil {
			return nil, err
		}
		pool.activeConnNum++
		return item, nil
	}
}

// 调用该方法时，pool.mu已经上锁
func (pool *Pool) getItem() (interface{}, error) {
	if pool.activeConnNum >= pool.MaxActiveNum { // 超过了最大活跃连接数
		return nil, ErrMaxActive
	}
	var err error
	for i := 0; i < pool.MaxRetryNum; i++ { // 最多重试三次
		item, err := pool.factory()
		if err == nil {
			return item, nil
		}
	}

	return nil, err
}

func (pool *Pool) Put(x interface{}) {
	if x == nil {
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed {
		pool.finalizer(x)
		pool.activeConnNum--
		return
	}

	select {
	case pool.idles <- x:
		pool.activeConnNum--
		return
	default:
		pool.finalizer(x)
		pool.activeConnNum--
	}
}

func (pool *Pool) Close() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.closed = true
	close(pool.idles)
	for item := range pool.idles {
		pool.finalizer(item)
		pool.activeConnNum--
	}
}
