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
	"time"

	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/zkhelper"
	. "github.com/pingcap/check"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct {
	zkConn zkhelper.Conn

	rootPath string
}

func (s *testUtilSuite) SetUpSuite(c *C) {
	conn, err := zkhelper.ConnectToZkWithTimeout(*testZKAddr, time.Second)
	c.Assert(err, IsNil)
	s.zkConn = conn

	s.rootPath = "/zk/tso_util_test"

	_, err = zkhelper.CreateRecursive(s.zkConn, s.rootPath, "", 0, zk.WorldACL(zkhelper.PERM_DIRECTORY))
	c.Assert(err, IsNil)
}

func (s *testUtilSuite) TearDownSuite(c *C) {
	if s.zkConn != nil {
		err := zkhelper.DeleteRecursive(s.zkConn, s.rootPath, -1)
		c.Assert(err, IsNil)

		s.zkConn.Close()
	}
}

func (s *testUtilSuite) TestTimestamp(c *C) {
	conn, err := zkhelper.ConnectToZkWithTimeout(*testZKAddr, time.Second)
	c.Assert(err, IsNil)
	defer conn.Close()

	tbl := []int64{
		1, 100, 1000,
	}

	for _, t := range tbl {
		err = saveTimestamp(conn, s.rootPath, t)
		c.Assert(err, IsNil)

		n, err := loadTimestamp(conn, s.rootPath)
		c.Assert(err, IsNil)
		c.Assert(n, Equals, t)
	}

	// test error
	_, err = loadTimestamp(conn, "error_root_path")
	c.Assert(err, NotNil)

	_, err = conn.Set(getTimestampPath(s.rootPath), []byte{}, -1)
	c.Assert(err, IsNil)

	_, err = loadTimestamp(conn, s.rootPath)
	c.Assert(err, NotNil)
}
