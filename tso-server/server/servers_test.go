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
	"encoding/json"
	"math/rand"
	"net"
	"path"
	"sync"
	"time"

	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
	"github.com/ngaut/zkhelper"
	. "github.com/pingcap/check"
)

var _ = Suite(&testMultiServerSuite{})

type testMultiServerSuite struct {
	rootPath string

	servers []*TimestampOracle

	zkConn zkhelper.Conn
}

func (s *testMultiServerSuite) SetUpSuite(c *C) {
	conn, err := zkhelper.ConnectToZkWithTimeout(*testZKAddr, time.Second)
	c.Assert(err, IsNil)
	s.zkConn = conn

	s.rootPath = "/zk/tso_multi_test"

	n := 3
	s.servers = make([]*TimestampOracle, 0, n)
	for i := 0; i < n; i++ {
		cfg := &Config{
			// use 127.0.0.1:0 to listen a unique port.
			Addr:     "127.0.0.1:0",
			ZKAddr:   *testZKAddr,
			RootPath: s.rootPath,
		}
		tso := testStartServer(c, cfg)
		s.servers = append(s.servers, tso)
	}
}

func (s *testMultiServerSuite) TearDownSuite(c *C) {
	for _, tso := range s.servers {
		tso.Close()
	}

	if s.zkConn != nil {
		err := zkhelper.DeleteRecursive(s.zkConn, s.rootPath, -1)
		c.Assert(err, IsNil)

		s.zkConn.Close()
	}
}

func (s *testMultiServerSuite) testGetTimestamp(c *C, duration time.Duration) {
	// get leader tso
	var (
		addr  string
		watch <-chan zk.Event
		err   error
		data  []byte

		conn net.Conn

		last proto.Response

		begin = time.Now()
	)

	for {
		if time.Now().Sub(begin) > duration {
			break
		}

		if len(addr) == 0 {
			data, _, watch, err = s.zkConn.GetW(path.Join(s.rootPath, "leader"))
			c.Assert(err, IsNil, Commentf("path %s", path.Join(s.rootPath, "leader")))

			var m map[string]interface{}
			err = json.Unmarshal(data, &m)
			c.Assert(err, IsNil)

			addr = m["Addr"].(string)

			conn, err = net.Dial("tcp", addr)
			c.Assert(err, IsNil)
		}

		_, err = conn.Write([]byte{0x00})
		if err == nil {
			var resp proto.Response
			err = resp.Decode(conn)
			if err == nil {
				c.Assert(resp.Physical, GreaterEqual, last.Physical)
				c.Assert(resp.Logical, Greater, last.Logical)
			}
		}

		if err != nil {
			conn.Close()

			// try connect the closed leader again, must error.
			conn, err = net.Dial("tcp", addr)
			if err == nil {
				conn.Write([]byte{0x00})
				var resp proto.Response
				err = resp.Decode(conn)
				conn.Close()
			}
			c.Assert(err, NotNil)

			// leader stop or close, we will wait some time for leader change.
			select {
			case <-watch:
				log.Warnf("leader changed")
			case <-time.After(500 * time.Millisecond):
				log.Warnf("wait change timeout, force get leader again")
			}

			addr = ""
		}
	}

	conn.Close()
}

func (s *testMultiServerSuite) TestMulti(c *C) {
	var wg sync.WaitGroup

	done := make(chan struct{}, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				for i, tso := range s.servers {
					if tso.IsLeader() {
						log.Warnf("handle leader %s", tso.ListenAddr())
						if rand.Int()%2 == 0 {
							// stop tso server
							tso.zkElector.Interrupt()
						} else {
							// restart tso server
							tso.Close()

							cfg := &Config{
								// use 127.0.0.1:0 to listen a unique port.
								Addr:     "127.0.0.1:0",
								ZKAddr:   *testZKAddr,
								RootPath: s.rootPath,
							}
							s.servers[i] = testStartServer(c, cfg)
						}

						break
					}
				}
			case <-done:
				return
			}
		}
	}()

	s.testGetTimestamp(c, 10*time.Second)

	close(done)
	wg.Wait()
}
