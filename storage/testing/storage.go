// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testing

import (
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tsuru/docker-cluster/cluster"
	cstorage "github.com/tsuru/docker-cluster/storage"
)

func assertIsNil(val interface{}, t *testing.T) {
	if val != nil {
		debug.PrintStack()
		t.Fatalf("Unexpected error: %s", val)
	}
}

func testStorageStoreRetrieveContainer(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveContainer("container-1")
	defer storage.RemoveContainer("container-2")
	defer storage.RemoveContainer("container-3")
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

type historyList []cluster.ImageHistory

func (l historyList) Len() int      { return len(l) }
func (l historyList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l historyList) Less(i, j int) bool {
	if l[i].Node == l[j].Node {
		return l[i].ImageId < l[j].ImageId
	}
	return l[i].Node < l[j].Node
}

func compareImage(img, expected cluster.Image, t *testing.T) {
	sort.Sort(historyList(img.History))
	sort.Sort(historyList(expected.History))
	if !reflect.DeepEqual(img, expected) {
		debug.PrintStack()
		t.Fatalf("unexpected image:\ngot: %#v\nexp: %#v", img, expected)
	}
}

func testStorageStoreRetrieveImage(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveImage("img-1", "id1", "host-1.something")
	defer storage.RemoveImage("img-1", "id1", "host-2")
	defer storage.RemoveImage("img-1", "id2", "host-2")
	err := storage.StoreImage("img-1", "id1", "host-1.something")
	assertIsNil(err, t)
	err = storage.StoreImage("img-1", "id1", "host-2")
	assertIsNil(err, t)
	img, err := storage.RetrieveImage("img-1")
	assertIsNil(err, t)
	expected := cluster.Image{Repository: "img-1", LastId: "id1", LastNode: "host-2", History: []cluster.ImageHistory{{
		Node:    "host-1.something",
		ImageId: "id1",
	}, {
		Node:    "host-2",
		ImageId: "id1",
	}}}
	compareImage(img, expected, t)
	err = storage.StoreImage("img-1", "id2", "host-2")
	assertIsNil(err, t)
	expected.History = append(expected.History, cluster.ImageHistory{Node: "host-2", ImageId: "id2"})
	expected.LastId = "id2"
	img, err = storage.RetrieveImage("img-1")
	assertIsNil(err, t)
	compareImage(img, expected, t)
}

func testStorageStoreImageIgnoreDups(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveImage("img-x", "id1", "host-1")
	err := storage.StoreImage("img-x", "id1", "host-1")
	assertIsNil(err, t)
	err = storage.StoreImage("img-x", "id1", "host-1")
	assertIsNil(err, t)
	img, err := storage.RetrieveImage("img-x")
	assertIsNil(err, t)
	expected := cluster.Image{Repository: "img-x", LastId: "id1", LastNode: "host-1", History: []cluster.ImageHistory{{
		Node:    "host-1",
		ImageId: "id1",
	}}}
	compareImage(img, expected, t)
}

func testStorageStoreRemoveImage(storage cluster.Storage, t *testing.T) {
	err := storage.StoreImage("img-1", "id1", "host-1")
	assertIsNil(err, t)
	err = storage.StoreImage("img-1", "id1", "host-2")
	assertIsNil(err, t)
	err = storage.StoreImage("img-1", "id2", "host-2")
	assertIsNil(err, t)
	expected := cluster.Image{Repository: "img-1", LastId: "id2", LastNode: "host-2", History: []cluster.ImageHistory{{
		Node:    "host-1",
		ImageId: "id1",
	}, {
		Node:    "host-2",
		ImageId: "id1",
	}, {
		Node:    "host-2",
		ImageId: "id2",
	}}}
	img, err := storage.RetrieveImage("img-1")
	assertIsNil(err, t)
	compareImage(img, expected, t)
	err = storage.RemoveImage("img-1", "id1", "host-1")
	assertIsNil(err, t)
	expected.History = []cluster.ImageHistory{{
		Node:    "host-2",
		ImageId: "id1",
	}, {
		Node:    "host-2",
		ImageId: "id2",
	}}
	img, err = storage.RetrieveImage("img-1")
	assertIsNil(err, t)
	compareImage(img, expected, t)
	err = storage.RemoveImage("img-1", "id1", "host-2")
	assertIsNil(err, t)
	expected.History = []cluster.ImageHistory{{
		Node:    "host-2",
		ImageId: "id2",
	}}
	img, err = storage.RetrieveImage("img-1")
	assertIsNil(err, t)
	compareImage(img, expected, t)
	err = storage.RemoveImage("img-1", "id2", "host-2")
	_, err = storage.RetrieveImage("img-1")
	if err != cstorage.ErrNoSuchImage {
		t.Fatalf("Expected error to be ErrNoSuchImage, got %s", err)
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
	if len(nodes) != 2 {
		t.Fatalf("unexpected number of nodes, expected 2, got: %d", len(nodes))
	}
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
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(100))
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
			locked, err := storage.LockNodeForHealing("addr-xyz", true, 5*time.Second)
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
	if dbNode.Healing.LockedUntil.IsZero() {
		t.Fatal("Expected node Healing.LockedUntil not to be zero")
	}
	if !dbNode.Healing.IsFailure {
		t.Fatal("Expected node healing.isFailure to be true")
	}
	dbNode.Healing = cluster.HealingData{}
	err = storage.UpdateNode(dbNode)
	assertIsNil(err, t)
	dbNode, err = storage.RetrieveNode("addr-xyz")
	assertIsNil(err, t)
	if !dbNode.Healing.LockedUntil.IsZero() {
		t.Fatal("Expected node Healing.LockedUntil to be zero")
	}
	if dbNode.Healing.IsFailure {
		t.Fatal("Expected node Healing.IsFailure to be false")
	}
}

func testStorageStoreAlreadyLocked(storage cluster.Storage, t *testing.T) {
	node1 := cluster.Node{
		Address:  "my-addr-locked",
		Metadata: map[string]string{},
		Healing:  cluster.HealingData{LockedUntil: time.Now().UTC().Add(5 * time.Second), IsFailure: true},
	}
	defer storage.RemoveNode("my-addr-locked")
	err := storage.StoreNode(node1)
	assertIsNil(err, t)
	nd, err := storage.RetrieveNode("my-addr-locked")
	assertIsNil(err, t)
	duration := nd.Healing.LockedUntil.Sub(node1.Healing.LockedUntil)
	if duration < 0 {
		duration = -duration
	}
	if duration > 1*time.Second {
		t.Errorf("unexpected node, expected: %#v, got: %#v", node1, nd)
	}
}

func testStorageLockNodeHealingAfterTimeout(storage cluster.Storage, t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(100))
	node := cluster.Node{Address: "addr-xyz"}
	defer storage.RemoveNode("addr-xyz")
	err := storage.StoreNode(node)
	assertIsNil(err, t)
	locked, err := storage.LockNodeForHealing("addr-xyz", true, 200*time.Millisecond)
	assertIsNil(err, t)
	locked, err = storage.LockNodeForHealing("addr-xyz", true, 200*time.Millisecond)
	assertIsNil(err, t)
	if locked {
		t.Fatal("Expected LockNodeForHealing to return false before timeout")
	}
	time.Sleep(300 * time.Millisecond)
	successCount := int32(0)
	wg := sync.WaitGroup{}
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			defer wg.Done()
			locked, err := storage.LockNodeForHealing("addr-xyz", true, 5*time.Second)
			assertIsNil(err, t)
			if locked {
				atomic.AddInt32(&successCount, 1)
			}
		}()
	}
	wg.Wait()
	if successCount != 1 {
		t.Fatalf("Expected LockNodeForHealing after timeout to lock only once, got: %d", successCount)
	}
}

func testStorageExtendNodeLock(storage cluster.Storage, t *testing.T) {
	node := cluster.Node{Address: "addr-xyz"}
	defer storage.RemoveNode("addr-xyz")
	err := storage.StoreNode(node)
	assertIsNil(err, t)
	locked, err := storage.LockNodeForHealing("addr-xyz", true, 200*time.Millisecond)
	assertIsNil(err, t)
	time.Sleep(300 * time.Millisecond)
	err = storage.ExtendNodeLock("addr-xyz", 200*time.Millisecond)
	assertIsNil(err, t)
	locked, err = storage.LockNodeForHealing("addr-xyz", true, 200*time.Millisecond)
	assertIsNil(err, t)
	if locked {
		t.Fatal("Expected LockNodeForHealing to return false after extending timeout")
	}
}

func testStorageUnlockNode(storage cluster.Storage, t *testing.T) {
	node := cluster.Node{Address: "addr-xyz"}
	defer storage.RemoveNode("addr-xyz")
	err := storage.StoreNode(node)
	assertIsNil(err, t)
	locked, err := storage.LockNodeForHealing("addr-xyz", true, 200*time.Millisecond)
	assertIsNil(err, t)
	err = storage.UnlockNode("addr-xyz")
	assertIsNil(err, t)
	locked, err = storage.LockNodeForHealing("addr-xyz", true, 200*time.Millisecond)
	assertIsNil(err, t)
	if !locked {
		t.Fatal("Expected LockNodeForHealing to return true after unlocking")
	}
}

func testRetrieveContainers(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveContainer("container-1")
	defer storage.RemoveContainer("container-2")
	defer storage.RemoveContainer("container-3")
	err := storage.StoreContainer("container-1", "host-1")
	assertIsNil(err, t)
	err = storage.StoreContainer("container-2", "host-1")
	assertIsNil(err, t)
	err = storage.StoreContainer("container-3", "host-2")
	assertIsNil(err, t)
	containers, err := storage.RetrieveContainers()
	assertIsNil(err, t)
	if len(containers) != 3 {
		t.Errorf("Unexpected len %d - expected %d", len(containers), 3)
	}
}

func testRetrieveImages(storage cluster.Storage, t *testing.T) {
	defer storage.RemoveImage("img-1", "id1", "host-1.something")
	defer storage.RemoveImage("img-1", "id1", "host-2")
	defer storage.RemoveImage("img-1", "id2", "host-2")
	err := storage.StoreImage("img-1", "id1", "host-1.something")
	assertIsNil(err, t)
	err = storage.StoreImage("img-1", "id1", "host-2")
	assertIsNil(err, t)
	imgs, err := storage.RetrieveImages()
	assertIsNil(err, t)
	if len(imgs) != 2 {
		t.Errorf("Unexpected len %d - expected %d", len(imgs), 2)
	}
}

func RunTestsForStorage(storage cluster.Storage, t *testing.T) {
	testStorageStoreRetrieveContainer(storage, t)
	testRetrieveContainers(storage, t)
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
	testStorageStoreAlreadyLocked(storage, t)
	testStorageLockNodeHealingAfterTimeout(storage, t)
	testStorageExtendNodeLock(storage, t)
	testStorageUnlockNode(storage, t)
	testRetrieveImages(storage, t)
}
