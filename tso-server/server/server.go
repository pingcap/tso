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
	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
	"github.com/ngaut/zkhelper"
)

const (
	// update timestamp every updateTimtestampStep milliseconds.
	updateTimtestampStep = int64(10)
	maxLogical           = int64(1 << 18)

	sessionReadBufferSize  = 8192
	sessionWriteBufferSize = 8192

	zkTimeout = 3 * time.Second

	maxConnChanSize = 100000

	defaultSaveInterval = 2000
)

type atomicObject struct {
	physical time.Time
	logical  int64
}

// TimestampOracle generates a unique timestamp.
type TimestampOracle struct {
	cfg *Config

	ts atomic.Value

	listener net.Listener

	wg       sync.WaitGroup
	connLock sync.Mutex
	conns    map[net.Conn]struct{}

	zkElector *zkhelper.ZElector
	zkConn    zkhelper.Conn

	isLeader int64

	lastSavedTime time.Time
}

func (tso *TimestampOracle) loadTimestamp() error {
	last, err := loadTimestamp(tso.zkConn, tso.cfg.RootPath)
	if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		// no timestamp node, create later
		err = nil
	}

	if err != nil {
		return errors.Trace(err)
	}

	var now time.Time

	for {
		now = time.Now()

		since := (now.UnixNano() - last) / 1e6
		if since <= 0 {
			return errors.Errorf("%s <= last saved time %s", now, time.Unix(0, last))
		}

		if wait := 2*tso.cfg.SaveInterval - since; wait > 0 {
			log.Warnf("wait %d milliseconds to guarantee valid generated timestamp", wait)
			time.Sleep(time.Duration(wait) * time.Millisecond)
			continue
		}

		break
	}

	if err = tso.saveTimestamp(now); err != nil {
		return errors.Trace(err)
	}

	current := &atomicObject{
		physical: now,
	}
	tso.ts.Store(current)

	return nil
}

func (tso *TimestampOracle) saveTimestamp(now time.Time) error {
	log.Debugf("save timestamp %s", now)
	err := saveTimestamp(tso.zkConn, tso.cfg.RootPath, now.UnixNano())
	if err != nil {
		return errors.Trace(err)
	}
	tso.lastSavedTime = now
	return nil
}

func (tso *TimestampOracle) updateTimestamp() error {
	prev := tso.ts.Load().(*atomicObject)
	now := time.Now()

	// ms
	since := now.Sub(prev.physical).Nanoseconds() / 1e6
	if since > 2*updateTimtestampStep {
		log.Warnf("clock offset: %v, prev: %v, now %v", since, prev.physical, now)
	}
	// Avoid the same physical time stamp
	if since <= 0 {
		log.Warn("invalid physical time stamp, re-update later again")
		return nil
	}

	if now.Sub(tso.lastSavedTime).Nanoseconds()/1e6 > tso.cfg.SaveInterval {
		if err := tso.saveTimestamp(now); err != nil {
			return errors.Trace(err)
		}
	}

	current := &atomicObject{
		physical: now,
	}
	tso.ts.Store(current)

	return nil
}

const maxRetryNum = 100

func (tso *TimestampOracle) getRespTS() *proto.Response {
	resp := &proto.Response{}
	for i := 0; i < maxRetryNum; i++ {
		current := tso.ts.Load().(*atomicObject)
		resp.Physical = int64(current.physical.UnixNano()) / 1e6
		resp.Logical = atomic.AddInt64(&current.logical, 1)
		if resp.Logical >= maxLogical {
			log.Errorf("logical part outside of max logical interval %v, please check ntp time", resp)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return resp
	}
	return nil
}

func (tso *TimestampOracle) handleConnection(s *session) {
	defer func() {
		tso.closeConn(s.conn)

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
		if resp == nil {
			log.Errorf("get repsone timestamp timeout, close %v", s.conn.RemoteAddr())
			return
		}
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

// NewTimestampOracle creates a tso server with special config.
func NewTimestampOracle(cfg *Config) (*TimestampOracle, error) {
	if cfg.SaveInterval <= 0 {
		cfg.SaveInterval = defaultSaveInterval
	}

	tso := &TimestampOracle{
		cfg:      cfg,
		isLeader: 0,
	}

	var err error
	tso.listener, err = net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(cfg.ZKAddr) == 0 {
		// no zookeeper, use a fake zk conn instead
		tso.zkConn = zkhelper.NewConn()
	} else {
		tso.zkConn, err = zkhelper.ConnectToZkWithTimeout(cfg.ZKAddr, zkTimeout)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	tso.conns = map[net.Conn]struct{}{}

	return tso, nil
}

type tsoTask struct {
	tso *TimestampOracle

	connCh chan net.Conn

	interruptCh chan error
	interrupted int64

	stopCh chan struct{}
}

func (t *tsoTask) Run() error {
	if t.Interrupted() {
		return errors.New("task has been interrupted already")
	}

	// enter here means I am the leader tso.
	tso := t.tso

	log.Debugf("tso leader %s run task", tso)

	if err := tso.loadTimestamp(); err != nil {
		// should we interrupt the task here now?
		return errors.Trace(err)
	}

	atomic.StoreInt64(&tso.isLeader, 1)

	tsTicker := time.NewTicker(time.Duration(updateTimtestampStep) * time.Millisecond)

	defer func() {
		atomic.StoreInt64(&tso.isLeader, 0)

		tsTicker.Stop()

		// we should close all connections.
		t.closeAllConns()
		log.Debugf("tso leader %s task end", tso)
	}()

	for {
		var conn net.Conn

		select {
		case err := <-t.interruptCh:
			log.Debugf("tso leader %s is interrupted, err %v", tso, err)

			// we will interrupt this task, and we can't run this task again.
			atomic.StoreInt64(&t.interrupted, 1)
			return errors.Trace(err)
		case <-t.stopCh:
			log.Debugf("tso leader %s is stopped", tso)

			// we will stop this task, maybe run again later.
			return errors.New("task is stopped")
		case conn = <-t.connCh:
			// handle connection below

			s := &session{
				r:    bufio.NewReaderSize(conn, sessionReadBufferSize),
				w:    bufio.NewWriterSize(conn, sessionWriteBufferSize),
				conn: conn,
			}

			tso.addConn(conn)
			tso.wg.Add(1)
			go tso.handleConnection(s)
		case <-tsTicker.C:
			if err := tso.updateTimestamp(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (t *tsoTask) Interrupted() bool {
	return atomic.LoadInt64(&t.interrupted) == 1
}

func (t *tsoTask) Stop() {
	select {
	case t.stopCh <- struct{}{}:
	default:
		log.Warnf("task stop blocked")
	}
}

func (t *tsoTask) closeAllConns() {
	t.tso.closeAllConns()

	// we may close the connection in connCh too
	n := len(t.connCh)
	for i := 0; i < n; i++ {
		conn := <-t.connCh
		conn.Close()
	}
}

func (t *tsoTask) Listening() {
	tso := t.tso

	defer tso.wg.Done()

	for {
		conn, err := tso.listener.Accept()
		if err != nil {
			t.interruptCh <- errors.Trace(err)
			return
		}

		if !tso.IsLeader() {
			// here mean I am not the leader now, so close all accpeted connections.
			conn.Close()
			continue
		}

		t.connCh <- conn
	}
}

// String implements fmt.Stringer interface.
func (tso *TimestampOracle) String() string {
	return tso.cfg.Addr
}

// Run runs the timestamp oracle server.
func (tso *TimestampOracle) Run() error {
	log.Debugf("tso %s begin to run", tso)

	m := map[string]interface{}{
		"Addr": tso.cfg.Addr,
	}
	ze, err := zkhelper.CreateElectionWithContents(tso.zkConn, tso.cfg.RootPath, m)
	if err != nil {
		return errors.Trace(err)
	}
	tso.zkElector = &ze

	task := &tsoTask{
		tso:         tso,
		connCh:      make(chan net.Conn, maxConnChanSize),
		interruptCh: make(chan error, 1),
		stopCh:      make(chan struct{}, 1),
		interrupted: 0,
	}

	tso.wg.Add(1)
	go task.Listening()

	err = tso.zkElector.RunTask(task)
	return errors.Trace(err)
}

// Close closes timestamp oracle server.
func (tso *TimestampOracle) Close() {
	log.Debugf("tso server %s closing", tso)

	if tso.listener != nil {
		tso.listener.Close()
	}

	tso.closeAllConns()

	if tso.zkConn != nil {
		tso.zkConn.Close()
	}

	tso.wg.Wait()

	return
}

// ListenAddr returns listen address.
// You must call it after tso server accepts succesfully.
func (tso *TimestampOracle) ListenAddr() string {
	return tso.listener.Addr().String()
}

// IsLeader returns whether tso is leader or not.
func (tso *TimestampOracle) IsLeader() bool {
	return atomic.LoadInt64(&tso.isLeader) == 1
}

func (tso *TimestampOracle) closeAllConns() {
	tso.connLock.Lock()
	for conn := range tso.conns {
		conn.Close()
		delete(tso.conns, conn)
	}
	tso.connLock.Unlock()
}

func (tso *TimestampOracle) addConn(conn net.Conn) {
	tso.connLock.Lock()
	tso.conns[conn] = struct{}{}
	tso.connLock.Unlock()
}

func (tso *TimestampOracle) closeConn(conn net.Conn) {
	tso.connLock.Lock()
	delete(tso.conns, conn)
	tso.connLock.Unlock()

	conn.Close()
}
