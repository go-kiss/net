package pool

import (
	"sync/atomic"
	"time"
)

var noDeadline = time.Time{}

type Conn struct {
	C Closer

	Inited    bool
	pooled    bool
	createdAt time.Time
	usedAt    atomic.Value
}

type Closer interface {
	Close() error
}

func newConn(netConn Closer) *Conn {
	cn := &Conn{
		C:         netConn,
		createdAt: time.Now(),
	}
	cn.setUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	return cn.usedAt.Load().(time.Time)
}

func (cn *Conn) setUsedAt(tm time.Time) {
	cn.usedAt.Store(tm)
}

func (cn *Conn) Close() error {
	return cn.C.Close()
}
