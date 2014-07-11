// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storage

import (
	"github.com/tsuru/docker-cluster/cluster"
	"reflect"
	"runtime/debug"
	"sort"
	"testing"
)

func assertIsNil(val interface{}, t *testing.T) {
	if val != nil {
		debug.PrintStack()
		t.Fatalf("Unexpected error: %s", val)
	}
}

func testStorageStoreRetrieveContainer(storage cluster.Storage, t *testing.T) {
	err := storage.StoreContainer("container-1", "host-1")
	assertIsNil(err, t)
	err = storage.StoreContainer("container-2", "host-1")
	assertIsNil(err, t)
	err = storage.StoreContainer("container-3", "host-2")
	assertIsNil(err, t)
	host, err := storage.RetrieveContainer("container-1")
	assertIsNil(err, t)
	if host != "host-1" {
		t.Errorf("Unexpected hostname %s - expected %s", host, "host-1")
	}
	host, err = storage.RetrieveContainer("container-2")
	assertIsNil(err, t)
	if host != "host-1" {
		t.Errorf("Unexpected hostname %s - expected %s", host, "host-1")
	}
	host, err = storage.RetrieveContainer("container-3")
	assertIsNil(err, t)
	if host != "host-2" {
		t.Errorf("Unexpected hostname %s - expected %s", host, "host-2")
	}
}

func testStorageStoreRemoveContainer(storage cluster.Storage, t *testing.T) {
	err := storage.StoreContainer("container-1", "host-9")
	assertIsNil(err, t)
	err = storage.RemoveContainer("container-1")
	assertIsNil(err, t)
	_, err = storage.RetrieveContainer("container-1")
	if err != ErrNoSuchContainer {
		t.Errorf("Error should be ErrNoSuchContainer, received: %s", err)
	}
}

func testStorageStoreRetrieveImage(storage cluster.Storage, t *testing.T) {
	err := storage.StoreImage("img-1", "host-1")
	assertIsNil(err, t)
	err = storage.StoreImage("img-1", "host-2")
	assertIsNil(err, t)
	err = storage.StoreImage("img-2", "host-2")
	assertIsNil(err, t)
	hosts, err := storage.RetrieveImage("img-1")
	assertIsNil(err, t)
	sort.Strings(hosts)
	if !reflect.DeepEqual(hosts, []string{"host-1", "host-2"}) {
		t.Errorf("unexpected array %#v", hosts)
	}
}

func testStorageStoreRemoveImage(storage cluster.Storage, t *testing.T) {
	err := storage.StoreImage("img-1", "host-1")
	assertIsNil(err, t)
	err = storage.StoreImage("img-1", "host-2")
	assertIsNil(err, t)
	err = storage.RemoveImage("img-1")
	assertIsNil(err, t)
	_, err = storage.RetrieveImage("img-1")
	if err != ErrNoSuchImage {
		t.Errorf("Error should be ErrNoSuchImage, received: %s", err)
	}
}

func testStorageStoreRetrieveNode(storage cluster.Storage, t *testing.T) {
	node1 := cluster.Node{ID: "id-1", Address: "my-addr-1"}
	defer storage.RemoveNode("id-1")
	err := storage.StoreNode(node1)
	assertIsNil(err, t)
	node2 := cluster.Node{ID: "id-2", Address: "my-addr-2"}
	defer storage.RemoveNode("id-2")
	err = storage.StoreNode(node2)
	assertIsNil(err, t)
	addr, err := storage.RetrieveNode("id-1")
	assertIsNil(err, t)
	if addr != "my-addr-1" {
		t.Errorf("addr should be my-addr-1, returned: %s", addr)
	}
	nodes, err := storage.RetrieveNodes()
	assertIsNil(err, t)
	if !((nodes[0] == node1 || nodes[0] == node2) && (nodes[1] == node1 || nodes[1] == node2)) {
		t.Errorf("unexpected nodes: %#v", nodes)
	}
}

func testStorageStoreRemoveNode(storage cluster.Storage, t *testing.T) {
	node1 := cluster.Node{ID: "id-1", Address: "my-addr-1"}
	err := storage.StoreNode(node1)
	assertIsNil(err, t)
	err = storage.RemoveNode("id-1")
	assertIsNil(err, t)
	_, err = storage.RetrieveNode("id-1")
	if err != ErrNoSuchNode {
		t.Errorf("Error should be ErrNoSuchNode, received: %s", err)
	}
	nodes, err := storage.RetrieveNodes()
	assertIsNil(err, t)
	if len(nodes) > 0 {
		t.Errorf("nodes should be empty, found: %#v", nodes)
	}
}

func runTestsForStorage(storage cluster.Storage, t *testing.T) {
	testStorageStoreRetrieveContainer(storage, t)
	testStorageStoreRemoveContainer(storage, t)
	testStorageStoreRetrieveImage(storage, t)
	testStorageStoreRemoveImage(storage, t)
	testStorageStoreRetrieveNode(storage, t)
	testStorageStoreRemoveNode(storage, t)
}
