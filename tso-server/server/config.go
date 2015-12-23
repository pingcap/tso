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

// Config is the configuration for timestamp oracle server
type Config struct {
	// Timestamp oracle server listen address
	Addr string

	// zookeeper address, format is zk1,zk2..., split by ,.
	// if empty, the tso server is just a standalone memory server, and
	// can't save lastest timestamp, so you may only use empty zk address in test.
	ZKAddr string

	// root path for saving data in zookeeper, the path must has the magic preifx /zk first,
	RootPath string
}
