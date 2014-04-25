// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/fsouza/go-dockerclient"
	"reflect"
	"testing"
)

func TestRoundRobinSchedule(t *testing.T) {
	scheduler := &roundRobin{stor: &mapStorage{}}
	scheduler.Register(map[string]string{"ID": "node0", "address": "url1"})
	scheduler.Register(map[string]string{"ID": "node1", "address": "url2"})
	opts := docker.CreateContainerOptions{Config: &docker.Config{}}
	node, err := scheduler.Schedule(opts, nil)
	if err != nil {
		t.Error(err)
	}
	if node.ID != "node0" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node0", node.ID)
	}
	node, _ = scheduler.Schedule(opts, nil)
	if node.ID != "node1" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node1", node.ID)
	}
	node, _ = scheduler.Schedule(opts, nil)
	if node.ID != "node0" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node0", node.ID)
	}
}

func TestNextEmpty(t *testing.T) {
	defer func() {
		expected := "No nodes available"
		r := recover().(string)
		if r != expected {
			t.Fatalf("next(): wrong panic message. Want %q. Got %q.", expected, r)
		}
	}()
	scheduler := &roundRobin{stor: &mapStorage{}}
	scheduler.next()
}

func TestRoundRobinNodes(t *testing.T) {
	nodes := []Node{
		{ID: "server0", Address: "http://localhost:8080"},
		{ID: "server1", Address: "http://localhost:8081"},
	}
	scheduler := &roundRobin{stor: &mapStorage{}}
	for _, n := range nodes {
		scheduler.Register(map[string]string{"address": n.Address, "ID": n.ID})
	}
	got, err := scheduler.Nodes()
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(got, nodes) {
		t.Errorf("roundRobin.Nodes(): wrong result. Want %#v. Got %#v.", nodes, got)
	}
}

func TestRoundRobinNodesUnregister(t *testing.T) {
	nodes := []Node{
		{ID: "server0", Address: "http://localhost:8080"},
		{ID: "server1", Address: "http://localhost:8081"},
	}
	scheduler := &roundRobin{stor: &mapStorage{}}
	for _, n := range nodes {
		scheduler.Register(map[string]string{"address": n.Address, "ID": n.ID})
	}
	scheduler.Unregister(map[string]string{"address": nodes[0].Address, "ID": nodes[0].ID})
	got, err := scheduler.Nodes()
	if err != nil {
		t.Error(err)
	}
	expected := []Node{{ID: "server1", Address: "http://localhost:8081"}}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("roundRobin.Nodes(): wrong result. Want %#v. Got %#v.", nodes, got)
	}
}
