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
	"encoding/binary"
	"flag"
	"net"
	"testing"
	"time"

	"github.com/ngaut/tso/proto"
	. "github.com/pingcap/check"
)

var testAddr = flag.String("addr", ":1234", "test tso server address")

func TestServer(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testServerSuite{})

type testServerSuite struct {
	tso *TimestampOracle
}

func (s *testServerSuite) SetUpSuite(c *C) {
	tso, err := NewTimestampOracle(*testAddr)
	c.Assert(err, IsNil)

	s.tso = tso

	go tso.Run()
	time.Sleep(100 * time.Millisecond)
}

func (s *testServerSuite) TearDownSuite(c *C) {
	s.tso.Close()
}

func (s *testServerSuite) testGetTimestamp(c *C, conn net.Conn, n int) []proto.Response {
	res := make([]proto.Response, n)

	_, err := conn.Write(make([]byte, n))
	c.Assert(err, IsNil)

	for i := 0; i < n; i++ {
		err = binary.Read(conn, binary.BigEndian, &res[i])
		c.Assert(err, IsNil)
	}

	return res
}

func (s *testServerSuite) TestServer(c *C) {
	conn, err := net.Dial("tcp", *testAddr)
	c.Assert(err, IsNil)
	defer conn.Close()

	res := s.testGetTimestamp(c, conn, 2)
	c.Assert(res[1].Physical, GreaterEqual, res[0].Physical)
	c.Assert(res[1].Logical, Greater, res[0].Logical)
}
