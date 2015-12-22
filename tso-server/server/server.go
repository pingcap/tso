// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
)

const (
	// update timestamp every updateTimtestampStep milliseconds.
	updateTimtestampStep = int64(10)
	maxLogical           = int64(1 << 18)

	sessionReadBufferSize  = 8192
	sessionWriteBufferSize = 8192
)

type atomicObject struct {
	physical time.Time
	logical  int64
}

// TimestampOracle generates a unique timestamp.
type TimestampOracle struct {
	ts     atomic.Value
	ticker *time.Ticker

	listener net.Listener

	wg       sync.WaitGroup
	connLock sync.Mutex
	conns    map[net.Conn]struct{}
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
			if since > 2*updateTimtestampStep {
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
	defer func() {
		tso.connLock.Lock()
		delete(tso.conns, s.conn)
		tso.connLock.Unlock()

		s.conn.Close()
		tso.wg.Done()
	}()

	var buf [1]byte

	for {
		_, err := s.r.Read(buf[:])
		if err != nil {
			log.Warn(err)
			return
		}

		resp := tso.getRespTS()
		resp.Encode(s.w)
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

// NewTimestampOracle creates a tso server with listen address addr.
func NewTimestampOracle(addr string) (*TimestampOracle, error) {
	tso := &TimestampOracle{
		ticker: time.NewTicker(time.Duration(updateTimtestampStep) * time.Millisecond),
	}
	current := &atomicObject{
		physical: time.Now(),
	}
	tso.ts.Store(current)
	go tso.updateTicker()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tso.listener = ln
	tso.conns = map[net.Conn]struct{}{}

	return tso, nil
}

// Run runs the timestamp oracle server.
func (tso *TimestampOracle) Run() {
	for {
		conn, err := tso.listener.Accept()
		if err != nil {
			return
		}

		s := &session{
			r:    bufio.NewReaderSize(conn, sessionReadBufferSize),
			w:    bufio.NewWriterSize(conn, sessionWriteBufferSize),
			conn: conn,
		}

		tso.conns[conn] = struct{}{}
		tso.wg.Add(1)
		go tso.handleConnection(s)
	}
}

// Close closes timestamp oracle server.
func (tso *TimestampOracle) Close() {
	tso.listener.Close()

	tso.connLock.Lock()
	for conn, _ := range tso.conns {
		conn.Close()
		delete(tso.conns, conn)
	}
	tso.connLock.Unlock()

	tso.wg.Wait()

	return
}
