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

func TestRedisStorageRetrieve(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"GET": []byte("server0")},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	host, err := storage.Retrieve(container)
	if err != nil {
		t.Error(err)
	}
	expectedHost := "server0"
	if host != expectedHost {
		t.Errorf("Retrieve(%q): want host %q. Got %q.", container, expectedHost, host)
	}
	cmd := conn.cmds[0]
	expectedCmd := "GET"
	if cmd.cmd != expectedCmd {
		t.Errorf("Retrieve(%q): want command %q. Got %q.", container, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{container}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("Retrieve(%q): want args %#v. Got %#v.", container, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRetrieveFailure(t *testing.T) {
	conn := failingFakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	_, err := storage.Retrieve(container)
	if err == nil {
		t.Errorf("Retrieve(%q): Got unexpected <nil> error", container)
	}
}

func TestRedisStorageRemove(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"DEL": int64(1)},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	err := storage.Remove(container)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "DEL"
	if cmd.cmd != expectedCmd {
		t.Errorf("Remove(%q): want command %q. Got %q.", container, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{container}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("Remove(%q): want args %#v. Got %#v.", container, expectedArgs, cmd.args)
	}
}

func TestRedisRemoveNoSuchContainer(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"DEL": int64(0)},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	err := storage.Remove(container)
	if err != ErrNoSuchContainer {
		t.Errorf("Remove(%q): wrong error. Want %#v. Got %#v.", container, ErrNoSuchContainer, err)
	}
}

func TestRedisNoAuthentication(t *testing.T) {
	var server redisServer
	err := server.start()
	if err != nil {
		t.Fatal(err)
	}
	defer server.stop()
	storage := Redis(server.addr())
	container := "affe3022"
	host := "server0"
	err = storage.Store(container, host)
	if err != nil {
		t.Error(err)
	}
	gotHost, err := storage.Retrieve(container)
	if err != nil {
		t.Error(err)
	}
	if gotHost != host {
		t.Errorf("Store and Retrieve returned wrong value. Want %q. Got %q.", host, gotHost)
	}
	err = storage.Remove(container)
	if err != nil {
		t.Error(err)
	}
}

func TestRedisStorageConnectionFailure(t *testing.T) {
	storage := Redis("something_unknown:39494")
	err := storage.Store("affe3022", "server0")
	if err == nil {
		t.Error("Got unexpected <nil> error")
	}
}

func TestRedisStorageAuthentication(t *testing.T) {
	var server redisServer
	server.password = "123456"
	err := server.start()
	if err != nil {
		t.Fatal(err)
	}
	defer server.stop()
	storage := AuthenticatedRedis(server.addr(), "123456")
	container := "affe3022"
	host := "server0"
	err = storage.Store(container, host)
	if err != nil {
		t.Error(err)
	}
	gotHost, err := storage.Retrieve(container)
	if err != nil {
		t.Error(err)
	}
	if gotHost != host {
		t.Errorf("Store and Retrieve returned wrong value. Want %q. Got %q.", host, gotHost)
	}
	err = storage.Remove(container)
	if err != nil {
		t.Error(err)
	}
}

func TestRedisStorageAuthenticationFailure(t *testing.T) {
	var server redisServer
	server.password = "123456"
	err := server.start()
	if err != nil {
		t.Fatal(err)
	}
	defer server.stop()
	storage := AuthenticatedRedis(server.addr(), "123")
	container := "affe3022"
	host := "server0"
	err = storage.Store(container, host)
	if err == nil {
		t.Error("Got unexpected <nil> error")
	}
}
