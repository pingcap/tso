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

package proto

import (
	"bytes"
	"testing"

	. "github.com/pingcap/check"
)

func TestProto(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testProtoSuite{})

type testProtoSuite struct {
}

func (s *testProtoSuite) TestProto(c *C) {
	var buf bytes.Buffer

	p := Response{}
	p.Physical = 1
	p.Logical = 1

	err := p.Encode(&buf)
	c.Assert(err, IsNil)

	var p2 Response
	err = p2.Decode(&buf)
	c.Assert(err, IsNil)

	c.Assert(p2, DeepEquals, p)

	err = p2.Decode(&buf)
	c.Assert(err, NotNil)
}
