// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import "testing"

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
