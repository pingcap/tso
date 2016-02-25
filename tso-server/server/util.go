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
	"path"

	"github.com/juju/errors"
	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/zkhelper"
)

func getTimestampPath(rootPath string) string {
	return path.Join(rootPath, "timestamp")
}

func loadTimestamp(conn zkhelper.Conn, rootPath string) (int64, error) {
	data, _, err := conn.Get(getTimestampPath(rootPath))
	if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		return 0, zk.ErrNoNode
	} else if err != nil {
		return 0, errors.Trace(err)
	} else if len(data) != 8 {
		return 0, errors.Errorf("invalid timestamp data, must 8 bytes, but %d", len(data))
	}

	return int64(binary.BigEndian.Uint64(data)), nil
}

func saveTimestamp(conn zkhelper.Conn, rootPath string, ts int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(ts))

	tsPath := getTimestampPath(rootPath)

	_, err := conn.Set(tsPath, buf[:], -1)
	if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		_, err = conn.Create(tsPath, buf[:], 0, zk.WorldACL(zkhelper.PERM_FILE))
	}

	return errors.Trace(err)
}
