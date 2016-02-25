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

package client

import (
	"flag"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/tso/proto"
	"github.com/ngaut/tso/tso-server/server"
	. "github.com/pingcap/check"
)

var testZK = flag.String("zk", "127.0.0.1:2181", "test zookeeper address")

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct {
	server   *server.TimestampOracle
	client   *Client
	rootPath string
}

func (s *testClientSuite) SetUpSuite(c *C) {
	s.testStartServer(c)

	s.client = NewClient(&Conf{
		ZKAddr:   *testZK,
		RootPath: s.rootPath,
	})
}

func (s *testClientSuite) TearDownSuite(c *C) {
	if s.server != nil {
		s.server.Close()
	}
}

func (s *testClientSuite) testStartServer(c *C) {
	s.rootPath = "/zk/test_tso"

	cfg := &server.Config{
		Addr:         "127.0.0.1:0",
		ZKAddr:       *testZK,
		RootPath:     s.rootPath,
		SaveInterval: 100,
	}
	svr, err := server.NewTimestampOracle(cfg)
	c.Assert(err, IsNil)

	cfg.Addr = svr.ListenAddr()

	go svr.Run()
	time.Sleep(100 * time.Millisecond)

	s.server = svr
}

func (s *testClientSuite) testGetTimestamp(c *C, n int) []*proto.Timestamp {
	res := make([]*proto.Timestamp, n)
	last := &proto.Timestamp{}
	for i := 0; i < n; i++ {
		r := s.client.GoGetTimestamp()
		ts, err := r.GetTS()
		c.Assert(err, IsNil)

		res[i] = ts
		c.Assert(ts.Physical, GreaterEqual, last.Physical)
		c.Assert(ts.Logical, Greater, last.Logical)
		last = ts
	}

	return res
}

func (s *testClientSuite) TestClient(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			s.testGetTimestamp(c, 10)
		}()
	}

	wg.Wait()
}

func (s *testClientSuite) TestRestart(c *C) {
	s.server.Close()

	n := 3
	for i := 0; i < n; i++ {
		r := s.client.GoGetTimestamp()
		_, err := r.GetTS()
		c.Assert(err, NotNil)
	}

	s.testStartServer(c)
	time.Sleep(2 * time.Second)

	for i := 0; i < n; i++ {
		r := s.client.GoGetTimestamp()
		_, err := r.GetTS()
		c.Assert(err, IsNil)
	}
}
