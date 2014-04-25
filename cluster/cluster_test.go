// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"testing"
)

func TestNewCluster(t *testing.T) {
	var tests = []struct {
		input []Node
		fail  bool
	}{
		{
			[]Node{{ID: "something", Address: "http://localhost:8083"}},
			false,
		},
		{
			[]Node{{ID: "something", Address: ""}, {ID: "otherthing", Address: "http://localhost:8083"}},
			true,
		},
		{
			[]Node{{ID: "something", Address: "http://localhost:8083"}},
			false,
		},
	}
	for _, tt := range tests {
		_, err := New(nil, &mapStorage{}, tt.input...)
		if tt.fail && err == nil || !tt.fail && err != nil {
			t.Errorf("cluster.New() for input %#v. Expect failure: %v. Got: %v.", tt.input, tt.fail, err)
		}
	}
}

func TestRegister(t *testing.T) {
	scheduler := &roundRobin{stor: &mapStorage{}}
	cluster, err := New(scheduler, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register(map[string]string{"ID": "abcdef", "address": "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	node := scheduler.next()
	if node.ID != "abcdef" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "abcdef", node.ID)
	}
	err = cluster.Register(map[string]string{"ID": "abcdefg", "address": "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	node = scheduler.next()
	if node.ID != "abcdefg" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "abcdefg", node.ID)
	}
	node = scheduler.next()
	if node.ID != "abcdef" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "abcdef", node.ID)
	}
}

func TestRegisterSchedulerUnableToRegister(t *testing.T) {
	var scheduler fakeScheduler
	cluster, err := New(scheduler, &mapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register(map[string]string{"ID": "abcdef", "address": ""})
	if err != ErrImmutableCluster {
		t.Error(err)
	}
}

func TestRegisterFailure(t *testing.T) {
	cluster, err := New(nil, &mapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register(map[string]string{"ID": "abcdef", "address": ""})
	if err == nil {
		t.Error("Expected non-nil error, got <nil>.")
	}
}

func TestUnregister(t *testing.T) {
	scheduler := &roundRobin{stor: &mapStorage{}}
	cluster, err := New(scheduler, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register(map[string]string{"ID": "abcdef", "address": "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Unregister(map[string]string{"ID": "abcdef", "address": "http://localhost:4243"})
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Should have recovered scheduler.next() panic.")
		}
	}()
	scheduler.next()
}

func TestUnregisterUnableToRegister(t *testing.T) {
	var scheduler fakeScheduler
	cluster, err := New(scheduler, &mapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Unregister(map[string]string{"ID": "abcdef", "address": ""})
	if err != ErrImmutableCluster {
		t.Error(err)
	}
}

func TestNodesShouldGetSchedulerNodes(t *testing.T) {
	cluster, err := New(nil, &mapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]string{"ID": "abcdef", "address": "http://localhost:4243"}
	err = cluster.Register(params)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Unregister(params)
	nodes, err := cluster.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	expected := []Node{{ID: "abcdef", Address: "http://localhost:4243"}}
	if !reflect.DeepEqual(nodes, expected) {
		t.Errorf("Expected nodes to be equal %q, got %q", expected, nodes)
	}
}

func TestNodesShouldReturnEmptyListWhenNoNodeIsFound(t *testing.T) {
	cluster, err := New(nil, &mapStorage{})
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
	storage := &mapStorage{cMap: map[string]string{id: "server0"}}
	cluster, err := New(nil, storage, Node{ID: "server0", Address: server.URL})
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
