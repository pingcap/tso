package main

import (
	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
	"net"
	"net/http"
	"net/rpc"
	"sync/atomic"
	"time"
)

const (
	step = 10
)

type atomicObject struct {
	physical time.Time
	logical  int64
}

type TimestampOracle struct {
	ts     atomic.Value
	ticker *time.Ticker
}

func (tso *TimestampOracle) updateTicker() {
	for {
		select {
		case <-tso.ticker.C:
			prev := tso.ts.Load().(*atomicObject)
			now := time.Now()
			// ms
			since := now.Sub(prev.physical).Nanoseconds() / 1024 / 1024
			if since > 2*step {
				log.Warnf("clock offset: %v, prev:%v, now %v", since, prev.physical, now)
			}
			current := &atomicObject{
				physical: now,
			}
			tso.ts.Store(current)
		}
	}

}

func (tso *TimestampOracle) Alloc(req *proto.Request, resp *proto.Response) error {
	prev := tso.ts.Load().(*atomicObject)
	resp.Physical = int64(prev.physical.Nanosecond()) / 1024 / 1024
	resp.Logical = atomic.AddInt64(&prev.logical, 1)
	return nil
}

func main() {
	tso := &TimestampOracle{
		ticker: time.NewTicker(10 * time.Millisecond),
	}
	current := &atomicObject{
		physical: time.Now(),
	}
	tso.ts.Store(current)
	go tso.updateTicker()
	rpc.Register(tso)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
