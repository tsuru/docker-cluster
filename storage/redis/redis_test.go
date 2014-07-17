// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redis

import (
	"github.com/garyburd/redigo/redis"
	"github.com/tsuru/docker-cluster/cluster"
	cstorage "github.com/tsuru/docker-cluster/storage"
	storageTesting "github.com/tsuru/docker-cluster/storage/testing"
	"reflect"
	"testing"
)

func TestRedisStorage(t *testing.T) {
	redis := Redis("localhost:6379", "test-docker-cluster")
	stor := redis.(*redisStorage)
	conn := stor.pool.Get()
	defer conn.Close()
	result, err := conn.Do("KEYS", "test-docker-cluster*")
	if err != nil {
		t.Fatal(err)
	}
	keys := result.([]interface{})
	for _, key := range keys {
		keyName := string(key.([]byte))
		conn.Do("DEL", keyName)
	}
	storageTesting.RunTestsForStorage(redis, t)
}

func TestRedisStorageStoreContainer(t *testing.T) {
	conn := fakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	host := "server0"
	err := storage.StoreContainer(container, host)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "SET"
	if cmd.cmd != expectedCmd {
		t.Errorf("StoreContainer(%q, %q): want command %q. Got %q.", container, host, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{container, host}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("StoreContainer(%q, %q): want args %#v. Got %#v.", container, host, expectedArgs, cmd.args)
	}
}

func TestRedisStorageStoreContainerPrefixed(t *testing.T) {
	conn := fakeConn{}
	storage := redisStorage{
		prefix: "docker",
		pool: redis.NewPool(func() (redis.Conn, error) {
			return &conn, nil
		}, 3),
	}
	container := "affe3022"
	host := "server0"
	err := storage.StoreContainer(container, host)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "SET"
	if cmd.cmd != expectedCmd {
		t.Errorf("StoreContainer(%q, %q): want command %q. Got %q.", container, host, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{"docker:" + container, host}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("StoreContainer(%q, %q): want args %#v. Got %#v.", container, host, expectedArgs, cmd.args)
	}
}

func TestRedisStorageStoreContainerFailure(t *testing.T) {
	conn := failingFakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	host := "server0"
	err := storage.StoreContainer(container, host)
	if err == nil {
		t.Error("Got unexpected <nil> error")
	}
}

func TestRedisStorageRetrieveContainer(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"GET": []byte("server0")},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	host, err := storage.RetrieveContainer(container)
	if err != nil {
		t.Error(err)
	}
	expectedHost := "server0"
	if host != expectedHost {
		t.Errorf("RetrieveContainer(%q): want host %q. Got %q.", container, expectedHost, host)
	}
	cmd := conn.cmds[0]
	expectedCmd := "GET"
	if cmd.cmd != expectedCmd {
		t.Errorf("RetrieveContainer(%q): want command %q. Got %q.", container, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{container}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("RetrieveContainer(%q): want args %#v. Got %#v.", container, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRetrieveContainerPrefixed(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"GET": []byte("server0")},
	}
	storage := redisStorage{
		pool: redis.NewPool(func() (redis.Conn, error) {
			return &conn, nil
		}, 3),
		prefix: "cluster",
	}
	container := "affe3022"
	_, err := storage.RetrieveContainer(container)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedArgs := []interface{}{"cluster:" + container}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("RetrieveContainer(%q): want args %#v. Got %#v.", container, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRetrieveContainerFailure(t *testing.T) {
	conn := failingFakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	_, err := storage.RetrieveContainer(container)
	if err == nil {
		t.Errorf("RetrieveContainer(%q): Got unexpected <nil> error", container)
	}
}

func TestRedisStorageRetrieveNoSuchContainer(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"GET": nil},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	_, err := storage.RetrieveContainer(container)
	if err != cstorage.ErrNoSuchContainer {
		t.Errorf("RetrieveContainer(%q): wrong error. Want %#v. Got %#v.", container, cstorage.ErrNoSuchContainer, err)
	}
}

func TestRedisStorageRemoveContainer(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"DEL": int64(1)},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	container := "affe3022"
	err := storage.RemoveContainer(container)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "DEL"
	if cmd.cmd != expectedCmd {
		t.Errorf("RemoveContainer(%q): want command %q. Got %q.", container, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{container}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("RemoveContainer(%q): want args %#v. Got %#v.", container, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRemoveContainerPrefixed(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"DEL": int64(1)},
	}
	storage := redisStorage{
		pool: redis.NewPool(func() (redis.Conn, error) {
			return &conn, nil
		}, 3),
		prefix: "leave",
	}
	container := "affe3022"
	err := storage.RemoveContainer(container)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedArgs := []interface{}{"leave:" + container}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("RemoveContainer(%q): want args %#v. Got %#v.", container, expectedArgs, cmd.args)
	}
}

func TestRedisRemoveContainerFailure(t *testing.T) {
	var conn failingFakeConn
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	err := storage.RemoveContainer("affe3022")
	if err == nil {
		t.Error("Unexpected <nil> error")
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
	err := storage.RemoveContainer(container)
	if err != cstorage.ErrNoSuchContainer {
		t.Errorf("RemoveContainer(%q): wrong error. Want %#v. Got %#v.", container, cstorage.ErrNoSuchContainer, err)
	}
}

func TestRedisStorageStoreImage(t *testing.T) {
	conn := fakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	image := "tsuru/python"
	host := "server0"
	err := storage.StoreImage(image, host)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "SADD"
	if cmd.cmd != expectedCmd {
		t.Errorf("StoreImage(%q, %q): want command %q. Got %q.", image, host, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{"image:" + image, host}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("StoreImage(%q, %q): want args %#v. Got %#v.", image, host, expectedArgs, cmd.args)
	}
}

func TestRedisStorageStoreImagePrefixed(t *testing.T) {
	conn := fakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	storage.prefix = "cluster"
	image := "tsuru/python"
	host := "server0"
	err := storage.StoreImage(image, host)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "SADD"
	if cmd.cmd != expectedCmd {
		t.Errorf("StoreImage(%q, %q): want command %q. Got %q.", image, host, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{"cluster:image:" + image, host}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("StoreImage(%q, %q): want args %#v. Got %#v.", image, host, expectedArgs, cmd.args)
	}
}

func TestRedisStorageStoreImageFailure(t *testing.T) {
	conn := failingFakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	image := "tsuru/python"
	host := "server0"
	err := storage.StoreImage(image, host)
	if err == nil {
		t.Error("Got unexpected <nil> error")
	}
}

func TestRedisStorageRetrieveImage(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"SMEMBERS": []interface{}{[]byte("server0")}},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	image := "tsuru/python"
	hosts, err := storage.RetrieveImage(image)
	if err != nil {
		t.Error(err)
	}
	expectedHosts := []string{"server0"}
	if !reflect.DeepEqual(hosts, expectedHosts) {
		t.Errorf("RetrieveImage(%q): want host %q. Got %q.", image, expectedHosts, hosts)
	}
	cmd := conn.cmds[0]
	expectedCmd := "SMEMBERS"
	if cmd.cmd != expectedCmd {
		t.Errorf("RetrieveImage(%q): want command %q. Got %q.", image, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{"image:" + image}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("RetrieveImage(%q): want args %#v. Got %#v.", image, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRetrieveImagePrefixed(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"SMEMBERS": []interface{}{[]byte("server0")}},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	storage.prefix = "cluster"
	image := "tsuru/python"
	_, err := storage.RetrieveImage(image)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "SMEMBERS"
	if cmd.cmd != expectedCmd {
		t.Errorf("RetrieveImage(%q): want command %q. Got %q.", image, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{"cluster:image:" + image}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("RetrieveImage(%q): want args %#v. Got %#v.", image, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRetrieveNoSuchImage(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	image := "tsuru/python"
	_, err := storage.RetrieveImage(image)
	if err != cstorage.ErrNoSuchImage {
		t.Errorf("RetrieveImage(%q): wrong error. Want %#v. Got %#v.", image, cstorage.ErrNoSuchImage, err)
	}
}

func TestRedisStorageRetrieveImageFailure(t *testing.T) {
	conn := failingFakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	image := "tsuru/python"
	_, err := storage.RetrieveImage(image)
	if err == nil {
		t.Error("Got unexpected <nil> error")
	}
}

func TestRedisStorageRemoveImage(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"DEL": int64(1)},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	image := "tsuru/python"
	err := storage.RemoveImage(image)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "DEL"
	if cmd.cmd != expectedCmd {
		t.Errorf("RemoveImage(%q): want command %q. Got %q.", image, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{"image:" + image}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("RemoveImage(%q): want args %#v. Got %#v.", image, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRemoveImagePrefixed(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"DEL": int64(1)},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	storage.prefix = "cluster"
	image := "tsuru/python"
	err := storage.RemoveImage(image)
	if err != nil {
		t.Error(err)
	}
	cmd := conn.cmds[0]
	expectedCmd := "DEL"
	if cmd.cmd != expectedCmd {
		t.Errorf("RemoveImage(%q): want command %q. Got %q.", image, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{"cluster:image:" + image}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("RemoveImage(%q): want args %#v. Got %#v.", image, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRemoveNoSuchImage(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"DEL": int64(0)},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	storage.prefix = "cluster"
	image := "tsuru/python"
	err := storage.RemoveImage(image)
	if err == nil {
		t.Error("Got unexpected <nil> error")
	}
}

func TestRedisStorageRemoveFailure(t *testing.T) {
	conn := failingFakeConn{}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	image := "tsuru/python"
	err := storage.RemoveImage(image)
	if err == nil {
		t.Error("Got unexpected <nil> error")
	}
}

func TestRedisNoAuthentication(t *testing.T) {
	var server redisServer
	err := server.start()
	if err != nil {
		t.Fatal(err)
	}
	defer server.stop()
	storage := Redis(server.addr(), "cluster")
	container := "affe3022"
	host := "server0"
	_, err = storage.RetrieveContainer(container)
	if err != cstorage.ErrNoSuchContainer {
		t.Errorf("RetrieveContainer(%q): wrong error. Want %#v. Got %#v", container, cstorage.ErrNoSuchContainer, err)
	}
	err = storage.RemoveContainer(container)
	if err != cstorage.ErrNoSuchContainer {
		t.Errorf("RemoveContainer(%q): wrong error. Want %#v. Got %#v", container, cstorage.ErrNoSuchContainer, err)
	}
	err = storage.StoreContainer(container, host)
	if err != nil {
		t.Error(err)
	}
	gotHost, err := storage.RetrieveContainer(container)
	if err != nil {
		t.Error(err)
	}
	if gotHost != host {
		t.Errorf("Store and Retrieve returned wrong value. Want %q. Got %q.", host, gotHost)
	}
	err = storage.RemoveContainer(container)
	if err != nil {
		t.Error(err)
	}
}

func TestRedisStorageConnectionFailure(t *testing.T) {
	storage := Redis("something_unknown:39494", "")
	err := storage.StoreContainer("affe3022", "server0")
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
	storage := AuthenticatedRedis(server.addr(), "123456", "docker")
	container := "affe3022"
	host := "server0"
	_, err = storage.RetrieveContainer(container)
	if err != cstorage.ErrNoSuchContainer {
		t.Errorf("RetrieveContainer(%q): wrong error. Want %#v. Got %#v", container, cstorage.ErrNoSuchContainer, err)
	}
	err = storage.RemoveContainer(container)
	if err != cstorage.ErrNoSuchContainer {
		t.Errorf("RemoveContainer(%q): wrong error. Want %#v. Got %#v", container, cstorage.ErrNoSuchContainer, err)
	}
	err = storage.StoreContainer(container, host)
	if err != nil {
		t.Error(err)
	}
	gotHost, err := storage.RetrieveContainer(container)
	if err != nil {
		t.Error(err)
	}
	if gotHost != host {
		t.Errorf("Store and Retrieve returned wrong value. Want %q. Got %q.", host, gotHost)
	}
	err = storage.RemoveContainer(container)
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
	storage := AuthenticatedRedis(server.addr(), "123", "docker")
	container := "affe3022"
	host := "server0"
	err = storage.StoreContainer(container, host)
	if err == nil {
		t.Error("Got unexpected <nil> error")
	}
}

func TestRedisStorageStoreNode(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply: map[string]interface{}{
			"SISMEMBER": interface{}(int64(0)),
		},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	address := "http://docker-node01.com:4243"
	err := storage.StoreNode(cluster.Node{Address: address})
	if err != nil {
		t.Errorf("Got unexpected %s error", err.Error)
	}
	cmd := conn.cmds[1]
	expectedCmd := "SADD"
	if cmd.cmd != expectedCmd {
		t.Errorf("StoreNode(%q): want command %q. Got %q.", address, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{"nodes", address}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("StoreNode(%q): want args %#v. Got %#v.", address, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRetrieveNodes(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply: map[string]interface{}{
			"SISMEMBER": interface{}(int64(0)),
			"SMEMBERS":  []interface{}{[]byte("http://docker-node01.com:4243")},
		},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	address := "http://docker-node01.com:4243"
	err := storage.StoreNode(cluster.Node{Address: address})
	if err != nil {
		t.Errorf("Got unexpected %s error", err.Error)
	}
	nodes, err := storage.RetrieveNodes()
	if err != nil {
		t.Errorf("Got unexpected %s error", err.Error)
	}
	expected := []cluster.Node{
		{Address: "http://docker-node01.com:4243", Metadata: map[string]string{}},
	}
	if !reflect.DeepEqual(nodes, expected) {
		t.Errorf("Expected nodes to be equal %q, got %q", expected, nodes)
	}
}

func TestRedisStorageRemoveNode(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"SREM": int64(1)},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	addr := "server01"
	err := storage.RemoveNode(addr)
	if err != nil {
		t.Errorf("Got unexpected %s error", err.Error)
	}
	cmd := conn.cmds[0]
	expectedCmd := "SREM"
	if cmd.cmd != expectedCmd {
		t.Errorf("RemoveNode(%q): want command %q. Got %q.", addr, expectedCmd, cmd.cmd)
	}
	expectedArgs := []interface{}{"nodes", addr}
	if !reflect.DeepEqual(cmd.args, expectedArgs) {
		t.Errorf("RemoveNode(%q): want args %#v. Got %#v.", addr, expectedArgs, cmd.args)
	}
}

func TestRedisStorageRemoveNodeFailure(t *testing.T) {
	var conn failingFakeConn
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	err := storage.RemoveNode("server01")
	if err == nil {
		t.Error("Unexpected <nil> error")
	}
}

func TestRedisStorageRemoveNodeNoSuchNode(t *testing.T) {
	conn := resultCommandConn{
		fakeConn: &fakeConn{},
		reply:    map[string]interface{}{"SREM": int64(0)},
	}
	var storage redisStorage
	storage.pool = redis.NewPool(func() (redis.Conn, error) {
		return &conn, nil
	}, 3)
	addr := "server01"
	err := storage.RemoveNode(addr)
	if err == nil {
		t.Errorf("Got unexpected <nil> error")
	}
	if err != cstorage.ErrNoSuchNode {
		t.Errorf("RemoveNode(%q): wrong error. Want %#v. Got %#v.", addr, cstorage.ErrNoSuchNode, err)
	}
}
