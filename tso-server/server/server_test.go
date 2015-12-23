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
	"flag"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/tso/proto"
	"github.com/ngaut/zkhelper"
	. "github.com/pingcap/check"
)

var testZKAddr = flag.String("zk", "127.0.0.1:2181", "test zookeeper address")

func TestServer(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testServerSuite{})

type testServerSuite struct {
	tso    *TimestampOracle
	zkConn zkhelper.Conn
}

func testStartServer(c *C, cfg *Config) *TimestampOracle {
	tso, err := NewTimestampOracle(cfg)
	c.Assert(err, IsNil)

	// reset address, because we use 127.0.0.1:0 for listening.
	tso.cfg.Addr = tso.ListenAddr()

	go tso.Run()
	time.Sleep(100 * time.Millisecond)
	return tso
}

func (s *testServerSuite) SetUpSuite(c *C) {
	conn, err := zkhelper.ConnectToZkWithTimeout(*testZKAddr, time.Second)
	c.Assert(err, IsNil)
	s.zkConn = conn

	cfg := &Config{
		Addr:     "127.0.0.1:0",
		ZKAddr:   *testZKAddr,
		RootPath: "/zk/tso_test",
	}

	s.tso = testStartServer(c, cfg)
}

func (s *testServerSuite) TearDownSuite(c *C) {
	if s.tso != nil {
		s.tso.Close()
	}

	if s.zkConn != nil {
		err := zkhelper.DeleteRecursive(s.zkConn, "/zk/tso_test", -1)
		c.Assert(err, IsNil)

		s.zkConn.Close()
	}
}

func (s *testServerSuite) testGetTimestamp(c *C, conn net.Conn, n int) []proto.Response {
	res := make([]proto.Response, n)

	_, err := conn.Write(make([]byte, n))
	c.Assert(err, IsNil)

	last := proto.Response{}
	for i := 0; i < n; i++ {
		err = res[i].Decode(conn)
		c.Assert(err, IsNil)

		c.Assert(res[i].Physical, GreaterEqual, last.Physical)
		c.Assert(res[i].Logical, Greater, last.Logical)
		last = res[i]
	}

	return res
}

func (s *testServerSuite) TestServer(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := net.Dial("tcp", s.tso.ListenAddr())
			c.Assert(err, IsNil)
			defer conn.Close()

			s.testGetTimestamp(c, conn, 10)
		}()
	}

	wg.Wait()
}

func (s *testServerSuite) TestFakeZK(c *C) {
	cfg := &Config{
		// use 127.0.0.1:0 to listen a unique port.
		Addr:     "127.0.0.1:0",
		RootPath: "/zk/tso_test",
	}
	tso := testStartServer(c, cfg)
	defer tso.Close()

	addr := tso.ListenAddr()

	conn, err := net.Dial("tcp", addr)
	c.Assert(err, IsNil)
	defer conn.Close()

	s.testGetTimestamp(c, conn, 10)
}
