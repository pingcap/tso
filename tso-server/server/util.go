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
	"encoding/json"
	"path"

	"github.com/juju/errors"
	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/zkhelper"
)

func getLeader(data []byte) (string, error) {
	m := struct {
		Addr string `json:"Addr"`
	}{}

	err := json.Unmarshal(data, &m)
	if err != nil {
		return "", errors.Trace(err)
	}

	return m.Addr, nil
}

// getLeaderPath gets the leader path in zookeeper.
func getLeaderPath(rootPath string) string {
	return path.Join(rootPath, "leader")
}

// func checkLeaderExists(conn zkhelper.Conn) error {
// 	// the leader node is not ephemeral, so we may meet no any tso server but leader node
// 	// has the data for last closed tso server.
// 	// TODO: check children in /candidates, if no child, we will treat it as no leader too.

// 	return nil
// }

// GetLeaderAddr gets the leader tso address in zookeeper for outer use.
func GetLeader(conn zkhelper.Conn, rootPath string) (string, error) {
	data, _, err := conn.Get(getLeaderPath(rootPath))
	if err != nil {
		return "", errors.Trace(err)
	}

	// if err != checkLeaderExists(conn); err != nil {
	// 	return "", errors.Trace(err)
	// }

	return getLeader(data)
}

// GetWatchLeader gets the leader tso address in zookeeper and returns a watcher for leader change.
func GetWatchLeader(conn zkhelper.Conn, rootPath string) (string, <-chan zk.Event, error) {
	data, _, watcher, err := conn.GetW(getLeaderPath(rootPath))
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	addr, err := getLeader(data)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	// if err != checkLeaderExists(conn); err != nil {
	// 	return "", errors.Trace(err)
	// }

	return addr, watcher, nil
}

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
	binary.BigEndian.PutUint64(buf[0:8], uint64(ts))

	tsPath := getTimestampPath(rootPath)

	_, err := conn.Set(tsPath, buf[0:8], -1)
	if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		_, err = conn.Create(tsPath, buf[0:8], 0, zk.WorldACL(zkhelper.PERM_FILE))
	}

	return errors.Trace(err)
}
