package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
)

var (
	addr       = flag.String("addr", ":1234", "server listening address")
	maxLogical = int64(math.Pow(2, 18))
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
	// TODO: save latest TS to persistent storage(zookeeper/etcd...)
	for {
		select {
		case <-tso.ticker.C:
			prev := tso.ts.Load().(*atomicObject)
			now := time.Now()

			// ms
			since := now.Sub(prev.physical).Nanoseconds() / 1e6
			if since > 2*step {
				log.Warnf("clock offset: %v, prev:%v, now %v", since, prev.physical, now)
			}
			// Avoid the same physical time stamp
			if since <= 0 {
				log.Warn("invalid physical time stamp")
				continue
			}

			current := &atomicObject{
				physical: now,
			}
			tso.ts.Store(current)
		}
	}
}

func (tso *TimestampOracle) getRespTS() *proto.Response {
	resp := &proto.Response{}
	for {
		current := tso.ts.Load().(*atomicObject)
		resp.Physical = int64(current.physical.UnixNano()) / 1e6
		resp.Logical = atomic.AddInt64(&current.logical, 1)
		if resp.Logical >= maxLogical {
			log.Errorf("logical part outside of max logical interval %v, please check ntp time", resp)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	return resp
}

func (tso *TimestampOracle) handleConnection(s *session) {
	var buf [1]byte
	defer s.conn.Close()

	for {
		_, err := s.r.Read(buf[:])
		if err != nil {
			log.Warn(err)
			return
		}

		resp := tso.getRespTS()
		binary.Write(s.w, binary.BigEndian, resp)
		if s.r.Buffered() <= 0 {
			err = s.w.Flush()
			if err != nil {
				log.Warn(err)
				return
			}
		}
	}
}

type session struct {
	r    *bufio.Reader
	w    *bufio.Writer
	conn net.Conn
}

func main() {
	flag.Parse()

	tso := &TimestampOracle{
		ticker: time.NewTicker(10 * time.Millisecond),
	}
	current := &atomicObject{
		physical: time.Now(),
	}
	tso.ts.Store(current)
	go tso.updateTicker()
	go http.ListenAndServe(":5555", nil)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Warning(err)
			continue
			// handle error
		}
		s := &session{
			r:    bufio.NewReaderSize(conn, 8192),
			w:    bufio.NewWriterSize(conn, 8192),
			conn: conn,
		}
		go tso.handleConnection(s)
	}
}
