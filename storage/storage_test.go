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

type NodeList []cluster.Node

func (a NodeList) Len() int           { return len(a) }
func (a NodeList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a NodeList) Less(i, j int) bool { return a[i].Address < a[j].Address }

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

func testStorageStoreRetrieveNodes(storage cluster.Storage, t *testing.T) {
	node1 := cluster.Node{Address: "my-addr-1"}
	defer storage.RemoveNode("my-addr-1")
	err := storage.StoreNode(node1)
	assertIsNil(err, t)
	node2 := cluster.Node{Address: "my-addr-2", Metadata: map[string]string{"foo": "bar"}}
	defer storage.RemoveNode("my-addr-2")
	err = storage.StoreNode(node2)
	assertIsNil(err, t)
	nodes, err := storage.RetrieveNodes()
	assertIsNil(err, t)
	sort.Sort(NodeList(nodes))
	if nodes[0].Address != node1.Address || nodes[1].Address != node2.Address {
		t.Errorf("unexpected nodes: %#v", nodes)
	}
	if !reflect.DeepEqual(node2.Metadata, nodes[1].Metadata) {
		t.Errorf("unexpected node metadata. expected: %#v got: %#v", node2.Metadata, nodes[1].Metadata)
	}
	if nodes[0].Metadata != nil && !reflect.DeepEqual(nodes[0].Metadata, map[string]string{}) {
		t.Errorf("unexpected node metadata. expected: %#v got: %#v", node1.Metadata, nodes[0].Metadata)
	}
}

func testStorageStoreRetrieveNodesForMetadaa(storage cluster.Storage, t *testing.T) {
	node1 := cluster.Node{Address: "my-addr-1", Metadata: map[string]string{
		"region": "reg1",
		"foo":    "bar",
	}}
	err := storage.StoreNode(node1)
	assertIsNil(err, t)
	defer storage.RemoveNode("my-addr-1")
	node2 := cluster.Node{Address: "my-addr-2", Metadata: map[string]string{
		"region": "reg2",
		"foo":    "bar",
	}}
	err = storage.StoreNode(node2)
	assertIsNil(err, t)
	defer storage.RemoveNode("my-addr-2")
	nodes, err := storage.RetrieveNodesByMetadata(map[string]string{"region": "reg2"})
	assertIsNil(err, t)
	if len(nodes) != 1 {
		t.Fatalf("unexpected nodes len: %d", len(nodes))
	}
	if nodes[0].Address != node2.Address {
		t.Errorf("unexpected node: %s", nodes[0].Address)
	}
	if !reflect.DeepEqual(node2.Metadata, nodes[0].Metadata) {
		t.Errorf("unexpected node metadata. expected: %#v got: %#v", node2.Metadata, nodes[0].Metadata)
	}
	nodes, err = storage.RetrieveNodesByMetadata(map[string]string{"foo": "bar"})
	assertIsNil(err, t)
	if len(nodes) != 2 {
		t.Fatalf("unexpected nodes len: %d", len(nodes))
	}
	sort.Sort(NodeList(nodes))
	if nodes[0].Address != node1.Address || nodes[1].Address != node2.Address {
		t.Errorf("unexpected nodes: %#v", nodes)
	}
	if !reflect.DeepEqual(node1.Metadata, nodes[0].Metadata) {
		t.Errorf("unexpected node metadata. expected: %#v got: %#v", node1.Metadata, nodes[0].Metadata)
	}
	if !reflect.DeepEqual(node2.Metadata, nodes[1].Metadata) {
		t.Errorf("unexpected node metadata. expected: %#v got: %#v", node2.Metadata, nodes[1].Metadata)
	}
}

func testStorageStoreRemoveNode(storage cluster.Storage, t *testing.T) {
	node1 := cluster.Node{Address: "my-addr-1"}
	err := storage.StoreNode(node1)
	assertIsNil(err, t)
	err = storage.RemoveNode("my-addr-1")
	assertIsNil(err, t)
	err = storage.RemoveNode("my-addr-1")
	if err != ErrNoSuchNode {
		t.Errorf("ErrNoSuchNode was expected, got: %s", err)
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
	testStorageStoreRetrieveNodes(storage, t)
	testStorageStoreRemoveNode(storage, t)
	testStorageStoreRetrieveNodesForMetadaa(storage, t)
}
