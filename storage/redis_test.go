// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storage

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
)

func TestRedisStorageStore(t *testing.T) {
	conn := fakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	host := "server0"
	err := storage.Store(container, host)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "SET"
	if cmd.cmd != expectedCmd {
		t.Errorf("Store(%q, %q): want command %q. Got %q.", container, host, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{container, host}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("Store(%q, %q): want args %#v. Got %#v.", container, host, expectedArgs, cmd.args)
	}
}

func TestRedisStorageStoreFailure(t *testing.T) {
	conn := failingFakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	host := "server0"
	err := storage.Store(container, host)
	if err == nil {
		t.Error("Got unexpected <nil> error")
	}
}
