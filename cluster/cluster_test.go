// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"math/rand"
	"net/http"
	"net/http/httptest"
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
	}
	for _, tt := range tests {
		_, err := New(tt.input...)
		if tt.fail && err == nil || !tt.fail && err != nil {
			t.Errorf("cluster.New(). Expect failure: %v. Got: %v.", tt.fail, err)
		}
	}
}

func TestNext(t *testing.T) {
	ids := []string{"abcdef", "abcdefg", "abcdefgh"}
	nodes := make([]Node, len(ids))
	for i, id := range ids {
		nodes[i] = Node{ID: id, Address: "http://localhost:4243"}
	}
	cluster, err := New(nodes...)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(ids)*2; i++ {
		expected := ids[i%len(ids)]
		node := cluster.next()
		if node.id != expected {
			t.Errorf("Wrong node from next call. Want %q. Got %q.", expected, node.id)
		}
	}
}

func TestRegister(t *testing.T) {
	cluster, err := New()
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register(Node{ID: "abcdef", Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	node := cluster.next()
	if node.id != "abcdef" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "abcdef", node.id)
	}
	err = cluster.Register(Node{ID: "abcdefg", Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	node = cluster.next()
	if node.id != "abcdefg" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "abcdefg", node.id)
	}
	node = cluster.next()
	if node.id != "abcdef" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "abcdef", node.id)
	}
}

func TestRegisterFailure(t *testing.T) {
	cluster, err := New()
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register(Node{ID: "abcdef", Address: ""})
	if err == nil {
		t.Error("Expected non-nil error, got <nil>.")
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
	cluster, err := New(Node{ID: "server0", Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	id := "e90302"
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
