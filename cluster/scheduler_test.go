// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
	dcli "github.com/fsouza/go-dockerclient"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestRoundRobinSchedule(t *testing.T) {
	body := `{"Id":"e90302"}`
	handler := []bool{false, false}
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler[0] = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler[1] = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	var scheduler roundRobin
	scheduler.Register(map[string]string{"ID": "node0", "address": server1.URL})
	scheduler.Register(map[string]string{"ID": "node1", "address": server2.URL})
	opts := dcli.CreateContainerOptions{}
	id, container, err := scheduler.Schedule(opts, &docker.Config{Memory: 67108864})
	if err != nil {
		t.Error(err)
	}
	if id != "node0" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node0", id)
	}
	if container.ID != "e90302" {
		t.Errorf("roundRobin.Schedule(): wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
	id, _, _ = scheduler.Schedule(opts, &docker.Config{Memory: 67108864})
	if id != "node1" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node1", id)
	}
	id, _, _ = scheduler.Schedule(opts, &docker.Config{Memory: 67108864})
	if id != "node0" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node0", id)
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
	var scheduler roundRobin
	scheduler.next()
}

func TestRoundRobinNodes(t *testing.T) {
	nodes := []Node{
		{ID: "server0", Address: "http://localhost:8080"},
		{ID: "server1", Address: "http://localhost:8081"},
	}
	var scheduler roundRobin
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

func TestRoundRobinNodesRegister(t *testing.T) {
	//params := map[string]string{"ID": "server0", "address": "http://localhost:8080"}
	//scheduler := &roundRobin{stor: storage.Redis("localhost:6379", "")}
	//err := scheduler.Register(params)
	//if err != nil {
	//t.Error(err)
	//}
}

func TestRoundRobinNodesUnregister(t *testing.T) {
	nodes := []Node{
		{ID: "server0", Address: "http://localhost:8080"},
		{ID: "server1", Address: "http://localhost:8081"},
	}
	var scheduler roundRobin
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
