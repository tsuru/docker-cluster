// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testing

import (
	"github.com/tsuru/docker-cluster/cluster"
	cstorage "github.com/tsuru/docker-cluster/storage"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
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
	if err != cstorage.ErrNoSuchContainer {
		t.Errorf("Error should be cstorage.ErrNoSuchContainer, received: %s", err)
	}
}

func testStorageStoreRetrieveImage(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveImage("img-1")
	defer storage.RemoveImage("img-2")
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

func testStorageStoreImageIgnoreDups(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveImage("img-x")
	err := storage.StoreImage("img-x", "host-1")
	assertIsNil(err, t)
	err = storage.StoreImage("img-x", "host-1")
	assertIsNil(err, t)
	hosts, err := storage.RetrieveImage("img-x")
	assertIsNil(err, t)
	if len(hosts) != 1 {
		t.Fatalf("Expected host list to have len 1, got: %d", len(hosts))
	}
	if hosts[0] != "host-1" {
		t.Fatalf("Expected host list to have value host-1, got: %s", hosts[0])
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
	if err != cstorage.ErrNoSuchImage {
		t.Errorf("Error should be cstorage.ErrNoSuchImage, received: %s", err)
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
	sort.Sort(cluster.NodeList(nodes))
	if nodes[0].Address != node1.Address || nodes[1].Address != node2.Address {
		t.Errorf("unexpected nodes: %#v", nodes)
	}
	if !reflect.DeepEqual(node2.Metadata, nodes[1].Metadata) {
		t.Errorf("unexpected node metadata. expected: %#v got: %#v", node2.Metadata, nodes[1].Metadata)
	}
	if !reflect.DeepEqual(nodes[0].Metadata, map[string]string{}) {
		t.Errorf("unexpected node metadata. expected empty map got: %#v", nodes[0].Metadata)
	}
}

func testStorageStoreRetrieveNode(storage cluster.Storage, t *testing.T) {
	node1 := cluster.Node{Address: "my-addr-1", Metadata: map[string]string{"abc": "def"}}
	defer storage.RemoveNode("my-addr-1")
	err := storage.StoreNode(node1)
	assertIsNil(err, t)
	nd, err := storage.RetrieveNode("my-addr-1")
	assertIsNil(err, t)
	if !reflect.DeepEqual(nd, node1) {
		t.Errorf("unexpected node, expected: %#v, got: %#v", node1, nd)
	}
	_, err = storage.RetrieveNode("my-addr-xxxx")
	if err != cstorage.ErrNoSuchNode {
		t.Errorf("Expected ErrNoSuchNode got: %#v", err)
	}
}

func testStorageStoreUpdateNode(storage cluster.Storage, t *testing.T) {
	node1 := cluster.Node{Address: "my-addr-1", Metadata: map[string]string{"abc": "def", "x": "y"}}
	defer storage.RemoveNode("my-addr-1")
	err := storage.StoreNode(node1)
	assertIsNil(err, t)
	delete(node1.Metadata, "x")
	node1.Metadata["ahoy"] = "foo"
	err = storage.UpdateNode(node1)
	assertIsNil(err, t)
	nd, err := storage.RetrieveNode("my-addr-1")
	if !reflect.DeepEqual(nd, node1) {
		t.Errorf("unexpected node, expected: %#v, got: %#v", node1, nd)
	}
	node1.Address = "my-addr-xxxxxx"
	err = storage.UpdateNode(node1)
	if err != cstorage.ErrNoSuchNode {
		t.Errorf("Expected ErrNoSuchNode got: %#v", err)
	}
}

func testStorageStoreEmptyMetadata(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveNode("my-addr-1")
	node1 := cluster.Node{Address: "my-addr-1", Metadata: map[string]string{}}
	err := storage.StoreNode(node1)
	assertIsNil(err, t)
	nodes, err := storage.RetrieveNodes()
	assertIsNil(err, t)
	if len(nodes) != 1 || nodes[0].Address != node1.Address {
		t.Errorf("unexpected nodes: %#v", nodes)
	}
}

func testStorageStoreRepeatedNodes(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveNode("my-addr-1")
	err := storage.StoreNode(cluster.Node{Address: "my-addr-1"})
	assertIsNil(err, t)
	err = storage.StoreNode(cluster.Node{Address: "my-addr-1"})
	if err != cstorage.ErrDuplicatedNodeAddress {
		t.Fatalf("Expected error cstorage.ErrDuplicatedNodeAddress, got: %#v", err)
	}
}

func testStorageStoreClearMetadata(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveNode("my-addr-1")
	err := storage.StoreNode(cluster.Node{Address: "my-addr-1", Metadata: map[string]string{"pool": "p1"}})
	assertIsNil(err, t)
	err = storage.RemoveNode("my-addr-1")
	assertIsNil(err, t)
	err = storage.StoreNode(cluster.Node{Address: "my-addr-1"})
	assertIsNil(err, t)
	nodes, err := storage.RetrieveNodes()
	assertIsNil(err, t)
	if len(nodes) != 1 || nodes[0].Address != "my-addr-1" {
		t.Errorf("unexpected nodes: %#v", nodes)
	}
	if !reflect.DeepEqual(nodes[0].Metadata, map[string]string{}) {
		t.Errorf("unexpected node metadata. expected empty map, got: %#v", nodes[0].Metadata)
	}
}

func testStorageStoreRetrieveNodesForMetadata(storage cluster.Storage, t *testing.T) {
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
	sort.Sort(cluster.NodeList(nodes))
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
	if err != cstorage.ErrNoSuchNode {
		t.Errorf("cstorage.ErrNoSuchNode was expected, got: %s", err)
	}
	nodes, err := storage.RetrieveNodes()
	assertIsNil(err, t)
	if len(nodes) > 0 {
		t.Errorf("nodes should be empty, found: %#v", nodes)
	}
}

func testStorageLockNodeHealing(storage cluster.Storage, t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(10))
	node := cluster.Node{Address: "addr-xyz"}
	defer storage.RemoveNode("addr-xyz")
	err := storage.StoreNode(node)
	assertIsNil(err, t)
	successCount := 0
	wg := sync.WaitGroup{}
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			defer wg.Done()
			locked, err := storage.LockNodeForHealing("addr-xyz")
			assertIsNil(err, t)
			if locked {
				successCount++
			}
		}()
	}
	wg.Wait()
	if successCount != 1 {
		t.Fatalf("Expected success in only one goroutine, got: %d", successCount)
	}
	dbNode, err := storage.RetrieveNode("addr-xyz")
	assertIsNil(err, t)
	if !dbNode.Healing {
		t.Fatal("Expected node healing to be true")
	}
}

func RunTestsForStorage(storage cluster.Storage, t *testing.T) {
	testStorageStoreRetrieveContainer(storage, t)
	testStorageStoreRemoveContainer(storage, t)
	testStorageStoreRetrieveImage(storage, t)
	testStorageStoreImageIgnoreDups(storage, t)
	testStorageStoreRemoveImage(storage, t)
	testStorageStoreRetrieveNodes(storage, t)
	testStorageStoreRepeatedNodes(storage, t)
	testStorageStoreRemoveNode(storage, t)
	testStorageStoreRetrieveNodesForMetadata(storage, t)
	testStorageStoreEmptyMetadata(storage, t)
	testStorageStoreClearMetadata(storage, t)
	testStorageStoreRetrieveNode(storage, t)
	testStorageStoreUpdateNode(storage, t)
	testStorageLockNodeHealing(storage, t)
}
