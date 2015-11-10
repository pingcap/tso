package main

import (
	"bufio"
	"encoding/binary"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
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

func (tso *TimestampOracle) handleConnection(s *session) {
	var buf [1]byte
	defer s.conn.Close()
	resp := &proto.Response{}
	for {
		_, err := s.r.Read(buf[:])
		if err != nil {
			log.Warn(err)
			return
		}
		prev := tso.ts.Load().(*atomicObject)
		resp.Physical = int64(prev.physical.Nanosecond()) / 1024 / 1024
		resp.Logical = atomic.AddInt64(&prev.logical, 1)
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
	tso := &TimestampOracle{
		ticker: time.NewTicker(10 * time.Millisecond),
	}
	current := &atomicObject{
		physical: time.Now(),
	}
	tso.ts.Store(current)
	go tso.updateTicker()
	go http.ListenAndServe(":5555", nil)

	ln, err := net.Listen("tcp", ":1234")
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
