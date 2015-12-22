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

var testAddr = flag.String("addr", ":1234", "test tso server address")

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct {
	server *server.TimestampOracle
	client *Client
}

func (s *testClientSuite) SetUpSuite(c *C) {
	s.testStartServer(c)

	s.client = NewClient(&Conf{
		ServerAddr: *testAddr,
	})
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.server.Close()
}

func (s *testClientSuite) testStartServer(c *C) {
	svr, err := server.NewTimestampOracle(*testAddr)
	c.Assert(err, IsNil)

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
		c.Assert(res[i].Physical, GreaterEqual, last.Physical)
		c.Assert(res[i].Logical, Greater, last.Logical)
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

	r := s.client.GoGetTimestamp()
	_, err := r.GetTS()
	c.Assert(err, NotNil)

	s.testStartServer(c)
	r = s.client.GoGetTimestamp()
	_, err = r.GetTS()
	c.Assert(err, IsNil)
}
