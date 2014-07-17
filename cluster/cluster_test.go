// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"errors"
	"github.com/fsouza/go-dockerclient"
	"github.com/tsuru/docker-cluster/storage"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"sort"
	"testing"
)

func TestNewCluster(t *testing.T) {
	var tests = []struct {
		input []Node
		fail  bool
	}{
		{
			[]Node{{Address: "http://localhost:8083"}},
			false,
		},
		{
			[]Node{{Address: ""}, {Address: "http://localhost:8083"}},
			true,
		},
		{
			[]Node{{Address: "http://localhost:8083"}},
			false,
		},
	}
	for _, tt := range tests {
		_, err := New(nil, &MapStorage{}, tt.input...)
		if tt.fail && err == nil || !tt.fail && err != nil {
			t.Errorf("cluster.New() for input %#v. Expect failure: %v. Got: %v.", tt.input, tt.fail, err)
		}
	}
}

func TestNewFailure(t *testing.T) {
	_, err := New(&roundRobin{}, nil)
	if err != errStorageMandatory {
		t.Fatalf("expected errStorageMandatory error, got: %#v", err)
	}
}

func TestRegister(t *testing.T) {
	scheduler := &roundRobin{}
	cluster, err := New(scheduler, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("http://localhost1:4243", nil)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.CreateContainerOptions{}
	node, err := scheduler.Schedule(cluster, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if node.Address != "http://localhost1:4243" {
		t.Errorf("Register failed. Got wrong Address. Want %q. Got %q.", "http://localhost1:4243", node.Address)
	}
	err = cluster.Register("http://localhost2:4243", nil)
	if err != nil {
		t.Fatal(err)
	}
	node, err = scheduler.Schedule(cluster, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if node.Address != "http://localhost2:4243" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "http://localhost2:4243", node.Address)
	}
	node, err = scheduler.Schedule(cluster, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if node.Address != "http://localhost1:4243" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "http://localhost1:4243", node.Address)
	}
}

func TestRegisterDoesNotAllowRepeatedAddresses(t *testing.T) {
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("http://localhost1:4243", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("http://localhost1:4243", nil)
	if err != storage.ErrDuplicatedNodeAddress {
		t.Fatalf("Expected error ErrDuplicatedNodeAddress, got: %#v", err)
	}
}

func TestRegisterFailure(t *testing.T) {
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("", nil)
	if err == nil {
		t.Error("Expected non-nil error, got <nil>.")
	}
}

func TestUnregister(t *testing.T) {
	scheduler := &roundRobin{}
	cluster, err := New(scheduler, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("http://localhost1:4243", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Unregister("http://localhost1:4243")
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.CreateContainerOptions{}
	_, err = scheduler.Schedule(cluster, opts, nil)
	if err == nil || err.Error() != "No nodes available" {
		t.Fatal("Expected no nodes available error")
	}
}

func TestNodesShouldGetClusterNodes(t *testing.T) {
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("http://localhost:4243", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Unregister("http://localhost:4243")
	nodes, err := cluster.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	expected := []Node{{Address: "http://localhost:4243", Metadata: map[string]string{}}}
	if !reflect.DeepEqual(nodes, expected) {
		t.Errorf("Expected nodes to be equal %q, got %q", expected, nodes)
	}
}

func TestNodesShouldGetClusterNodesWithoutDisabledNodes(t *testing.T) {
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Unregister("http://server1:4243")
	defer cluster.Unregister("http://server2:4243")
	err = cluster.Register("http://server1:4243", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("http://server2:4243", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.handleNodeError("http://server1:4243", errors.New("some err"))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := cluster.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	expected := []Node{
		{Address: "http://server2:4243", Metadata: map[string]string{}},
	}
	if !reflect.DeepEqual(nodes, expected) {
		t.Errorf("Expected nodes to be equal %q, got %q", expected, nodes)
	}
}

func TesteUnfilteredNodesReturnAllNodes(t *testing.T) {
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Unregister("http://server1:4243")
	defer cluster.Unregister("http://server2:4243")
	err = cluster.Register("http://server1:4243", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("http://server2:4243", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.handleNodeError("http://server1:4243", errors.New("some err"))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := cluster.UnfilteredNodes()
	if err != nil {
		t.Fatal(err)
	}
	expected := []Node{
		{Address: "http://server1:4243", Metadata: map[string]string{}},
		{Address: "http://server2:4243", Metadata: map[string]string{}},
	}
	sort.Sort(NodeList(nodes))
	if !reflect.DeepEqual(nodes, expected) {
		t.Errorf("Expected nodes to be equal %q, got %q", expected, nodes)
	}
}

func TestNodesForMetadataShouldGetClusterNodesWithMetadata(t *testing.T) {
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("http://server1:4243", map[string]string{"key1": "val1"})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register("http://server2:4243", map[string]string{"key1": "val2"})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Unregister("http://server1:4243")
	defer cluster.Unregister("http://server2:4243")
	nodes, err := cluster.NodesForMetadata(map[string]string{"key1": "val2"})
	if err != nil {
		t.Fatal(err)
	}
	expected := []Node{{Address: "http://server2:4243", Metadata: map[string]string{"key1": "val2"}}}
	if !reflect.DeepEqual(nodes, expected) {
		t.Errorf("Expected nodes to be equal %q, got %q", expected, nodes)
	}
}

func TestNodesShouldReturnEmptyListWhenNoNodeIsFound(t *testing.T) {
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := cluster.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Errorf("Expected nodes to be empty, got %q", nodes)
	}
}

func TestRunOnNodesStress(t *testing.T) {
	n := 1000
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(16))
	body := `{"Id":"e90302","Path":"date","Args":[]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server.Close()
	id := "e90302"
	stor := &MapStorage{}
	err := stor.StoreContainer(id, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, stor, Node{Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rand.Intn(10)+n; i++ {
		container, err := cluster.InspectContainer(id)
		if err != nil {
			t.Fatal(err)
		}
		if container.ID != id {
			t.Errorf("InspectContainer(%q): Wrong ID. Want %q. Got %q.", id, id, container.ID)
		}
		if container.Path != "date" {
			t.Errorf("InspectContainer(%q): Wrong Path. Want %q. Got %q.", id, "date", container.Path)
		}
	}
}

func TestClusterNodes(t *testing.T) {
	c, err := New(&roundRobin{}, &MapStorage{})
	if err != nil {
		t.Fatalf("unexpected error %s", err.Error())
	}
	nodes := []Node{
		{Address: "http://localhost:8080", Metadata: map[string]string{}},
		{Address: "http://localhost:8081", Metadata: map[string]string{}},
	}
	for _, n := range nodes {
		c.Register(n.Address, nil)
	}
	got, err := c.Nodes()
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(got, nodes) {
		t.Errorf("roundRobin.Nodes(): wrong result. Want %#v. Got %#v.", nodes, got)
	}
}

func TestClusterNodesUnregister(t *testing.T) {
	c, err := New(&roundRobin{}, &MapStorage{})
	if err != nil {
		t.Fatalf("unexpected error %s", err.Error())
	}
	nodes := []Node{
		{Address: "http://localhost:8080"},
		{Address: "http://localhost:8081"},
	}
	for _, n := range nodes {
		c.Register(n.Address, nil)
	}
	c.Unregister(nodes[0].Address)
	got, err := c.Nodes()
	if err != nil {
		t.Error(err)
	}
	expected := []Node{{Address: "http://localhost:8081", Metadata: map[string]string{}}}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("roundRobin.Nodes(): wrong result. Want %#v. Got %#v.", nodes, got)
	}
}
