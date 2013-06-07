// Copyright 2013 Globo.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"testing"
)

func TestNewCluster(t *testing.T) {
	var tests = []struct {
		input []Node
		fail  bool
	}{
		{
			[]Node{{Id: "something", Address: "http://localhost:8083"}},
			false,
		},
		{
			[]Node{{Id: "something", Address: ""}, {Id: "otherthing", Address: "http://localhost:8083"}},
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
		nodes[i] = Node{Id: id, Address: "http://localhost:4243"}
	}
	cluster, err := New(nodes...)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(ids) * 2; i++ {
		expected := ids[i % len(ids)]
		node := cluster.next()
		if node.id != expected {
			t.Errorf("Wrong node from next call. Want %q. Got %q.", expected, node.id)
		}
	}
}
